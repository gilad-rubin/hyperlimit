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
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import Any, Awaitable, Callable

OnChange = Callable[[int, int, str], None]

_GROWTH_MODES = ("fixed", "sqrt")


class AdaptiveLimiter:
    """A resizable concurrency gate driven by AIMD.

    - additive increase: after `successes_per_increase` recorded successes,
      the limit grows — by 1 (`growth="fixed"`) or by ~sqrt of the current
      limit (`growth="sqrt"`, which ramps wide lanes fast while staying
      gentle near small limits) — never past `cap`.
    - multiplicative decrease: a recorded throttle multiplies the limit by
      `decrease_factor` (never below `floor`), then a cooldown window
      absorbs the rest of the same burst — one storm means one cut, not a
      freefall. The window is `retry_after` when the caller passes a sane
      value (0 < retry_after <= `max_retry_after`), else `cooldown_seconds`,
      optionally stretched by up to `jitter` of itself so parallel lanes or
      processes don't resume in lockstep.
    - pause on throttle: while the window is open, `acquire` admits nothing
      new (`pause_on_throttle=False` restores halve-only behavior).

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
        jitter: float = 0.0,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
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
        if not (0.0 <= jitter <= 1.0):
            raise ValueError(f"jitter must be in [0, 1], got {jitter}")
        if max_retry_after <= 0:
            raise ValueError(f"max_retry_after must be positive, got {max_retry_after}")
        self._limit = initial
        self._floor = floor
        self._cap = cap
        self._successes_per_increase = successes_per_increase
        self._decrease_factor = decrease_factor
        self._cooldown_seconds = cooldown_seconds
        self._max_retry_after = max_retry_after
        self._pause_on_throttle = pause_on_throttle
        self._growth = growth
        self._jitter = jitter
        self._clock = clock
        self._sleep = sleep
        self._rng = rng
        self._on_change = on_change
        self._active = 0
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

    async def acquire(self) -> None:
        cond = self._condition()
        async with cond:
            while self._active >= self._limit or self._admissions_paused():
                await cond.wait()
            self._active += 1

    async def release(self) -> None:
        cond = self._condition()
        async with cond:
            self._active -= 1
            cond.notify_all()

    # ── verdicts ────────────────────────────────────────────────────────

    async def record_success(self) -> None:
        cond = self._condition()
        async with cond:
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
            if self._pause_on_throttle:
                self._spawn_waker(window)

    # ── introspection ───────────────────────────────────────────────────

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def active(self) -> int:
        return self._active

    @property
    def paused(self) -> bool:
        return self._admissions_paused()

    def snapshot(self) -> dict[str, int | bool]:
        return {
            "limit": self._limit,
            "active": self._active,
            "successes": self._successes,
            "paused": self._admissions_paused(),
        }

    # ── internals ───────────────────────────────────────────────────────

    def _condition(self) -> asyncio.Condition:
        if self._cond is None:
            self._cond = asyncio.Condition()
        return self._cond

    def _admissions_paused(self) -> bool:
        return (
            self._pause_on_throttle
            and self._cooldown_until is not None
            and self._clock() < self._cooldown_until
        )

    def _spawn_waker(self, delay: float) -> None:
        task = asyncio.get_running_loop().create_task(self._wake_after(delay))
        self._wakers.add(task)
        task.add_done_callback(self._wakers.discard)

    async def _wake_after(self, delay: float) -> None:
        await self._sleep(delay)
        cond = self._condition()
        async with cond:
            cond.notify_all()

    def _set_limit(self, new: int, reason: str) -> None:
        old = self._limit
        if new == old:
            return
        self._limit = new
        if self._on_change is not None:
            self._on_change(old, new, reason)
