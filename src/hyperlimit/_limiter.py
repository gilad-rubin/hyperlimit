"""AIMD concurrency limiter: additive increase, multiplicative decrease.

The permit and the verdict are separate on purpose. `async with limiter`
bounds how many workers run at once; `record_success` / `record_throttle`
teach the limiter whether the workload is healthy. Only the caller knows
what a throttle signal looks like for its provider (a 429, a connection
storm, a gateway timeout), so the limiter never guesses.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Callable

OnChange = Callable[[int, int, str], None]


class AdaptiveLimiter:
    """A resizable concurrency gate driven by AIMD.

    - additive increase: after `successes_per_increase` recorded successes,
      the limit grows by 1 (never past `cap`).
    - multiplicative decrease: a recorded throttle multiplies the limit by
      `decrease_factor` (never below `floor`), then a cool-down of
      `cooldown_seconds` absorbs the rest of the same burst — one storm
      means one cut, not a freefall.

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
        clock: Callable[[], float] = time.monotonic,
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
        self._limit = initial
        self._floor = floor
        self._cap = cap
        self._successes_per_increase = successes_per_increase
        self._decrease_factor = decrease_factor
        self._cooldown_seconds = cooldown_seconds
        self._clock = clock
        self._on_change = on_change
        self._active = 0
        self._successes = 0
        self._last_decrease: float | None = None
        self._cond: asyncio.Condition | None = None

    # ── permit ──────────────────────────────────────────────────────────

    async def __aenter__(self) -> "AdaptiveLimiter":
        await self.acquire()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.release()

    async def acquire(self) -> None:
        cond = self._condition()
        async with cond:
            while self._active >= self._limit:
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
                self._set_limit(self._limit + 1, "additive-increase")
                cond.notify_all()

    async def record_throttle(self) -> None:
        cond = self._condition()
        async with cond:
            now = self._clock()
            if (
                self._last_decrease is not None
                and now - self._last_decrease < self._cooldown_seconds
            ):
                return
            self._last_decrease = now
            self._successes = 0
            self._set_limit(
                max(self._floor, int(self._limit * self._decrease_factor)),
                "multiplicative-decrease",
            )

    # ── introspection ───────────────────────────────────────────────────

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def active(self) -> int:
        return self._active

    def snapshot(self) -> dict[str, int]:
        return {"limit": self._limit, "active": self._active, "successes": self._successes}

    # ── internals ───────────────────────────────────────────────────────

    def _condition(self) -> asyncio.Condition:
        if self._cond is None:
            self._cond = asyncio.Condition()
        return self._cond

    def _set_limit(self, new: int, reason: str) -> None:
        old = self._limit
        if new == old:
            return
        self._limit = new
        if self._on_change is not None:
            self._on_change(old, new, reason)
