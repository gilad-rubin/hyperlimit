"""AIMD concurrency limiter: additive increase, multiplicative decrease.

The permit and the verdict are separate on purpose. `async with limiter`
bounds how many workers run at once; `record_success` / `record_throttle`
teach the limiter whether the workload is healthy. Only the caller knows
what a throttle signal looks like for its provider (a 429, a connection
storm, a gateway timeout), so the limiter never guesses.

A throttle opens a cooldown window that does three things at once: absorbs
further throttles from the same burst into the one cut, pauses NEW
admissions until it ends (permits already held keep running), and — when the
caller passes a sane server `retry_after` — lasts exactly as long as the
server asked. Waiters parked by the pause are woken by a timer task, so the
`sleep` used for it is injectable alongside `clock` for deterministic tests.

Growth needs evidence: a success counts toward the next increase only while
demand (permits held plus callers waiting) fills at least `growth_threshold`
of the limit. A quiet lane that succeeds one call at a time has not shown
the limit is too low, so it stays where it is instead of creeping to `cap`
and firing the next burst into a storm.

A wait can be bounded: past `max_wait` seconds `acquire` raises
`AdmissionTimeout` and holds nothing, so a caller inside a request budget (a
gateway's 240 s, a client's 480 s) can fail with a clear reason instead of
outliving the request it serves.
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import Any, Awaitable, Callable

from hyperlimit._observe import (
    Admitted,
    LimitChanged,
    Released,
    Throttled,
    TimedOut,
    _active_observer,
    _deliver,
)

OnChange = Callable[[int, int, str], None]
Clock = Callable[[], float]
Sleep = Callable[[float], Awaitable[None]]

_GROWTH_MODES = ("fixed", "sqrt")


class AdmissionTimeout(TimeoutError):
    """No permit within `max_wait`. The caller holds nothing.

    Attributes:
        waited: Seconds spent waiting.
        queued: Callers already waiting when this one arrived.
        limit: The limit when the wait ended.
        limiter: The limiter's `name=`, if it was given one.
        lane: The `PartitionedLimiter` lane, if the wait was through one.
    """

    def __init__(
        self, *, waited: float, queued: int, limit: int, limiter: str | None, lane: str | None
    ) -> None:
        where = lane or limiter or "limiter"
        super().__init__(
            f"no permit from {where!r} within {waited:.3g}s "
            f"(limit {limit}, {queued} waiting ahead)"
        )
        self.waited = waited
        self.queued = queued
        self.limit = limit
        self.limiter = limiter
        self.lane = lane

    def __reduce__(self) -> tuple[Any, ...]:
        # Keyword-only fields: rebuild from them, so the error survives pickling
        # (checkpointed run results, process pools).
        fields = {
            "waited": self.waited, "queued": self.queued, "limit": self.limit,
            "limiter": self.limiter, "lane": self.lane,
        }
        return (_rebuild_admission_timeout, (fields,))


class AdaptiveLimiter:
    """A resizable concurrency gate driven by AIMD.

    - additive increase: after `successes_per_increase` recorded successes
      under load (demand at least `growth_threshold` of the limit; 0 counts
      every success), the limit grows — by 1 (`growth="fixed"`) or by ~sqrt
      of the current limit (`growth="sqrt"`, which ramps wide lanes fast
      while staying gentle near small limits) — never past `cap`.
    - multiplicative decrease: a recorded throttle multiplies the limit by
      `decrease_factor` (never below `floor`), then a cooldown window
      absorbs the rest of the same burst — one storm means one cut, not a
      freefall. The window is `retry_after` when the caller passes a sane
      value (0 < retry_after <= `max_retry_after`), else `cooldown_seconds`,
      optionally stretched by up to `jitter` of itself so parallel lanes or
      processes don't resume in lockstep.
    - pause on throttle: while the window is open, `acquire` admits nothing
      new (`pause_on_throttle=False` restores halve-only behavior).
    - bounded wait: `max_wait` (seconds, None = unbounded) caps how long
      `acquire` waits before raising `AdmissionTimeout`; `acquire(max_wait=)`
      overrides it per call.

    `name` labels this limiter's telemetry (`observe_limits`) and errors.

    The internal condition binds to the event loop that first awaits it;
    create one limiter per process/loop (tests reset between loops).
    """

    def __init__(
        self,
        *,
        initial: int = 3,
        floor: int = 1,
        cap: int = 16,
        successes_per_increase: int = 10,
        decrease_factor: float = 0.5,
        cooldown_seconds: float = 30.0,
        max_retry_after: float = 60.0,
        pause_on_throttle: bool = True,
        growth: str = "fixed",
        growth_threshold: float = 0.5,
        jitter: float = 0.0,
        max_wait: float | None = None,
        name: str | None = None,
        clock: Clock = time.monotonic,
        sleep: Sleep = asyncio.sleep,
        rng: Callable[[], float] = random.random,
        on_change: OnChange | None = None,
    ) -> None:
        if not (1 <= floor <= initial <= cap):
            raise ValueError(
                f"need 1 <= floor <= initial <= cap, got floor={floor} initial={initial} cap={cap}"
            )
        if not (0.0 < decrease_factor < 1.0):
            raise ValueError(f"decrease_factor must be in (0, 1), got {decrease_factor}")
        if successes_per_increase < 1:
            raise ValueError("successes_per_increase must be >= 1")
        if growth not in _GROWTH_MODES:
            raise ValueError(f"growth must be one of {_GROWTH_MODES}, got {growth!r}")
        if not (0.0 <= growth_threshold <= 1.0):
            raise ValueError(f"growth_threshold must be in [0, 1], got {growth_threshold}")
        if not (0.0 <= jitter <= 1.0):
            raise ValueError(f"jitter must be in [0, 1], got {jitter}")
        if max_retry_after <= 0:
            raise ValueError(f"max_retry_after must be positive, got {max_retry_after}")
        _check_max_wait(max_wait)
        self._limit = initial
        self._floor = floor
        self._cap = cap
        self._successes_per_increase = successes_per_increase
        self._decrease_factor = decrease_factor
        self._cooldown_seconds = cooldown_seconds
        self._max_retry_after = max_retry_after
        self._pause_on_throttle = pause_on_throttle
        self._growth = growth
        self._growth_threshold = growth_threshold
        self._jitter = jitter
        self._max_wait = max_wait
        self._name = name
        self._clock = clock
        self._sleep = sleep
        self._rng = rng
        self._on_change = on_change
        self._active = 0
        self._waiting = 0
        self._successes = 0
        self._cooldown_until: float | None = None
        self._cond: asyncio.Condition | None = None
        self._wakers: set[asyncio.Task[None]] = set()

    # ── permit ──────────────────────────────────────────────────────────

    async def __aenter__(self) -> "AdaptiveLimiter":
        await self.acquire()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.release()

    async def acquire(self, *, max_wait: float | None = None) -> None:
        """Take one permit, waiting while the limit is full or admissions pause.

        `max_wait` bounds this call's wait, overriding the constructor's; past
        it `AdmissionTimeout` is raised and no permit is held.
        """
        started = self._clock()
        queued = self._waiting
        admitted = await self._take_permit(self._deadline(started, max_wait))
        self._report_admission(admitted, started=started, queued=queued, lane=None)

    async def release(self) -> None:
        await self._return_permit()
        self._report_release(lane=None)

    # ── verdicts ────────────────────────────────────────────────────────

    async def record_success(self) -> None:
        cond = self._condition()
        async with cond:
            if not self._under_load():
                return  # an idle success is no evidence the limit is too low
            self._successes += 1
            if self._successes >= self._successes_per_increase and self._limit < self._cap:
                self._successes = 0
                step = 1 if self._growth == "fixed" else max(1, int(self._limit**0.5))
                self._set_limit(min(self._cap, self._limit + step), "additive-increase")
                cond.notify_all()

    async def record_throttle(self, *, retry_after: float | None = None) -> None:
        cond = self._condition()
        async with cond:
            now = self._clock()
            if self._cooldown_until is not None and now < self._cooldown_until:
                self._report_throttle(retry_after, window=None)
                return
            window = self._cooldown_seconds
            if retry_after is not None and 0 < retry_after <= self._max_retry_after:
                window = retry_after
            window += window * self._jitter * self._rng()
            self._cooldown_until = now + window
            self._successes = 0
            self._set_limit(
                max(self._floor, int(self._limit * self._decrease_factor)),
                "multiplicative-decrease",
            )
            self._report_throttle(retry_after, window=window)
            if self._pause_on_throttle:
                self._spawn_waker(window)

    # ── introspection ───────────────────────────────────────────────────

    @property
    def name(self) -> str | None:
        return self._name

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def active(self) -> int:
        return self._active

    @property
    def waiting(self) -> int:
        return self._waiting

    @property
    def paused(self) -> bool:
        return self._admissions_paused()

    def snapshot(self) -> dict[str, int | bool]:
        return {
            "limit": self._limit,
            "active": self._active,
            "waiting": self._waiting,
            "successes": self._successes,
            "paused": self._admissions_paused(),
        }

    # ── internals (PartitionedLimiter drives these for its lanes) ───────

    def _deadline(self, started: float, max_wait: float | None) -> float | None:
        _check_max_wait(max_wait)
        wait = self._max_wait if max_wait is None else max_wait
        return None if wait is None else started + wait

    async def _take_permit(self, deadline: float | None) -> bool:
        cond = self._condition()
        async with cond:
            admitted = await _wait_until(
                cond,
                self._has_room,
                deadline=deadline,
                clock=self._clock,
                sleep=self._sleep,
                on_wait=self._note_waiting,
            )
            if admitted:
                self._active += 1
            return admitted

    async def _return_permit(self) -> None:
        cond = self._condition()
        async with cond:
            self._active -= 1
            cond.notify_all()

    def _note_waiting(self, delta: int) -> None:
        self._waiting += delta

    def _report_admission(
        self, admitted: bool, *, started: float, queued: int, lane: str | None
    ) -> None:
        waited = self._clock() - started
        event = Admitted if admitted else TimedOut
        observer = _active_observer()
        if observer is not None:
            _deliver(observer, event(self._name, lane, waited, queued, self._limit, self._active))
        if not admitted:
            raise AdmissionTimeout(
                waited=waited, queued=queued, limit=self._limit, limiter=self._name, lane=lane
            )

    def _report_release(self, *, lane: str | None) -> None:
        observer = _active_observer()
        if observer is not None:
            _deliver(observer, Released(self._name, lane, self._limit, self._active, self._waiting))

    def _report_throttle(self, retry_after: float | None, *, window: float | None) -> None:
        observer = _active_observer()
        if observer is not None:
            _deliver(observer, Throttled(self._name, retry_after, window, self._limit))

    def _condition(self) -> asyncio.Condition:
        if self._cond is None:
            self._cond = asyncio.Condition()
        return self._cond

    def _has_room(self) -> bool:
        return self._active < self._limit and not self._admissions_paused()

    def _under_load(self) -> bool:
        return self._active + self._waiting >= self._growth_threshold * self._limit

    def _admissions_paused(self) -> bool:
        return (
            self._pause_on_throttle
            and self._cooldown_until is not None
            and self._clock() < self._cooldown_until
        )

    def _spawn_waker(self, delay: float) -> None:
        task = asyncio.get_running_loop().create_task(
            _notify_after(self._condition(), delay, self._sleep)
        )
        self._wakers.add(task)
        task.add_done_callback(self._wakers.discard)

    def _set_limit(self, new: int, reason: str) -> None:
        old = self._limit
        if new == old:
            return
        self._limit = new
        if self._on_change is not None:
            self._on_change(old, new, reason)
        observer = _active_observer()
        if observer is not None:
            _deliver(observer, LimitChanged(self._name, old, new, reason))


async def _wait_until(
    cond: asyncio.Condition,
    ready: Callable[[], bool],
    *,
    deadline: float | None,
    clock: Clock,
    sleep: Sleep,
    on_wait: Callable[[int], None],
) -> bool:
    """Wait on `cond` (already held) until `ready()`; False once `deadline` passes.

    The caller counts as waiting (`on_wait(+1)` / `on_wait(-1)`) only while
    parked. A deadline arms a timer that wakes the condition when it passes —
    re-armed if it fires a hair early — through the injectable `sleep`, so
    timeouts stay deterministic in tests.
    """
    if ready():
        return True
    timer: asyncio.Task[None] | None = None
    on_wait(+1)
    try:
        while not ready():
            if deadline is not None:
                remaining = deadline - clock()
                if remaining <= 0:
                    return False
                if timer is None or timer.done():
                    timer = asyncio.get_running_loop().create_task(
                        _notify_after(cond, remaining, sleep)
                    )
            await cond.wait()
        return True
    finally:
        on_wait(-1)
        if timer is not None:
            timer.cancel()


async def _notify_after(cond: asyncio.Condition, delay: float, sleep: Sleep) -> None:
    await sleep(delay)
    async with cond:
        cond.notify_all()


def _rebuild_admission_timeout(fields: dict[str, Any]) -> AdmissionTimeout:
    return AdmissionTimeout(**fields)


def _check_max_wait(max_wait: float | None) -> None:
    if max_wait is not None and max_wait <= 0:
        raise ValueError(f"max_wait must be positive or None, got {max_wait}")
