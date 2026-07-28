"""A capacity-one token bucket for requests-per-second pacing."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable


class RequestPacer:
    """Admit requests at a steady rate without limiting their concurrency.

    The capacity-one bucket allows one immediate request after an idle period,
    then reserves one slot every ``1 / requests_per_second`` seconds. Concurrent
    callers reserve slots under a lock and sleep independently; admission does
    not require a release when the provider operation finishes.
    """

    def __init__(
        self,
        requests_per_second: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if requests_per_second <= 0:
            raise ValueError(
                f"requests_per_second must be positive, got {requests_per_second}"
            )
        # Keep the theoretical boundary just outside the rolling window even
        # when binary floating-point rounds repeated reservations downward.
        self._interval = (1 / requests_per_second) * (1 + 1e-12)
        self._clock = clock
        self._sleep = sleep
        self._next_at: float | None = None
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = self._clock()
            admitted_at = now if self._next_at is None else max(now, self._next_at)
            self._next_at = admitted_at + self._interval

        delay = admitted_at - now
        if delay > 0:
            await self._sleep(delay)
