"""Token budget: a continuous-refill bucket for rate limits counted in tokens.

LLM providers throttle on tokens-per-minute more than on concurrency, so a
permit count alone admits bursts the quota cannot absorb. `spend(estimate)`
before a call debits the bucket (waiting for refill when dry); `adjust(delta)`
afterwards reconciles the estimate against actual usage. A request bigger
than a full bucket is admitted when the bucket is full and leaves a debt,
which honestly delays followers instead of deadlocking the caller.
"""

from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable


class TokenBudget:
    def __init__(
        self,
        tokens_per_minute: float,
        *,
        capacity: float | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if tokens_per_minute <= 0:
            raise ValueError(f"tokens_per_minute must be positive, got {tokens_per_minute}")
        self._rate = tokens_per_minute / 60.0
        self._capacity = capacity if capacity is not None else tokens_per_minute
        self._available = self._capacity
        self._clock = clock
        self._sleep = sleep
        self._last: float | None = None

    async def spend(self, tokens: float) -> None:
        while True:
            self._refill()
            need = min(tokens, self._capacity)
            if self._available >= need:
                self._available -= tokens
                return
            await self._sleep((need - self._available) / self._rate)

    def adjust(self, delta: float) -> None:
        """Reconcile an estimate: positive refunds over-estimated tokens,
        negative charges under-estimated ones."""
        self._refill()
        self._available = min(self._capacity, self._available + delta)

    @property
    def available(self) -> float:
        self._refill()
        return self._available

    def _refill(self) -> None:
        now = self._clock()
        if self._last is not None:
            self._available = min(
                self._capacity, self._available + (now - self._last) * self._rate
            )
        self._last = now
