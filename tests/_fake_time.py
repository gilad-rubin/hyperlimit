"""Deterministic clock + sleep pair for limiter tests: no real waiting.

`advance` moves the clock and resolves every sleeper whose wake time has
arrived. Callers must yield to the loop (``await asyncio.sleep(0)``) after
advancing so woken tasks actually run.
"""

from __future__ import annotations

import asyncio


class FakeTime:
    def __init__(self, start: float = 0.0) -> None:
        self.now = start
        self._sleepers: list[tuple[float, asyncio.Future[None]]] = []

    def clock(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._sleepers.append((self.now + seconds, future))
        await future

    def advance(self, seconds: float) -> None:
        self.now += seconds
        due = [entry for entry in self._sleepers if entry[0] <= self.now]
        for entry in due:
            self._sleepers.remove(entry)
            if not entry[1].done():
                entry[1].set_result(None)

    @property
    def pending_sleeps(self) -> list[float]:
        return [wake for wake, _ in self._sleepers]


async def drain() -> None:
    """Give woken tasks a few scheduler turns without real waiting."""
    for _ in range(10):
        await asyncio.sleep(0)
