"""Partition one adaptive limit across named lanes that share a quota.

Two independent AIMD limiters on one provider quota fight: one lane's
throttle teaches the other nothing, so it keeps pushing into the same 429
storm and both oscillate. A PartitionedLimiter keeps ONE limiter (one limit,
one AIMD loop, one pause) and gives each lane a reserved share of it —
`max(1, round(share * limit))`, recomputed from the live limit at every
admission, so shares track the adaptive limit as it moves.

Verdicts go through the lanes and reach the one shared limiter: any lane's
throttle shrinks and pauses everyone (that is the point). Reserves are
strict, not work-conserving — an idle lane's reserve is headroom the parent
cap simply never lends out; rounding can make reserves sum past the limit,
and the parent's own permit count is what bounds the true total. Size the
parent floor at or above the lane count so every lane keeps a live reserve.

A caller waiting for its lane's reserve is demand on the shared limit, so it
counts toward the parent's growth threshold and queue depth. A lane's
`max_wait` (the parent's by default) covers both waits — for the reserve,
then for the shared permit — as one deadline.
"""

from __future__ import annotations

import asyncio
from typing import Any

from hyperlimit._limiter import AdaptiveLimiter, _wait_until


class PartitionedLimiter:
    def __init__(self, limiter: AdaptiveLimiter, shares: dict[str, float]) -> None:
        if not shares:
            raise ValueError("shares must name at least one lane")
        if any(share <= 0 for share in shares.values()):
            raise ValueError(f"every share must be positive, got {shares}")
        if sum(shares.values()) > 1.0 + 1e-9:
            raise ValueError(f"shares must sum to at most 1.0, got {shares}")
        self._limiter = limiter
        self._shares = dict(shares)
        self._active = {name: 0 for name in shares}
        self._waiting = {name: 0 for name in shares}
        self._lanes = {name: Lane(self, name) for name in shares}
        self._cond: asyncio.Condition | None = None

    def lane(self, name: str) -> "Lane":
        return self._lanes[name]

    def reserved(self, name: str) -> int:
        return max(1, round(self._shares[name] * self._limiter.limit))

    @property
    def limit(self) -> int:
        return self._limiter.limit

    def snapshot(self) -> dict[str, Any]:
        return {
            **self._limiter.snapshot(),
            "lanes": {
                name: {
                    "active": self._active[name],
                    "waiting": self._waiting[name],
                    "reserved": self.reserved(name),
                }
                for name in self._shares
            },
        }

    async def record_success(self) -> None:
        await self._limiter.record_success()
        cond = self._condition()
        async with cond:
            cond.notify_all()  # a grown limit grows reserves; wake lane waiters

    async def record_throttle(self, *, retry_after: float | None = None) -> None:
        await self._limiter.record_throttle(retry_after=retry_after)

    # ── internals ───────────────────────────────────────────────────────

    async def _acquire(self, name: str, max_wait: float | None) -> None:
        parent = self._limiter
        started = parent._clock()
        queued = parent.waiting
        deadline = parent._deadline(started, max_wait)
        cond = self._condition()

        def note_waiting(delta: int) -> None:
            self._waiting[name] += delta
            parent._note_waiting(delta)

        async with cond:
            admitted = await _wait_until(
                cond,
                lambda: self._active[name] < self.reserved(name),
                deadline=deadline,
                clock=parent._clock,
                sleep=parent._sleep,
                on_wait=note_waiting,
            )
            if admitted:
                self._active[name] += 1
        if admitted:
            try:
                admitted = await parent._take_permit(deadline)  # total bound + pause
            except BaseException:
                await self._drop(name)
                raise
            if not admitted:
                await self._drop(name)
        parent._report_admission(admitted, started=started, queued=queued, lane=name)

    async def _release(self, name: str) -> None:
        await self._limiter._return_permit()
        await self._drop(name)
        self._limiter._report_release(lane=name)

    async def _drop(self, name: str) -> None:
        cond = self._condition()
        async with cond:
            self._active[name] -= 1
            cond.notify_all()

    def _condition(self) -> asyncio.Condition:
        if self._cond is None:
            self._cond = asyncio.Condition()
        return self._cond


class Lane:
    """One partition's view: the permit surface of the shared limiter."""

    def __init__(self, partition: PartitionedLimiter, name: str) -> None:
        self._partition = partition
        self._name = name

    async def __aenter__(self) -> "Lane":
        await self.acquire()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.release()

    async def acquire(self, *, max_wait: float | None = None) -> None:
        await self._partition._acquire(self._name, max_wait)

    async def release(self) -> None:
        await self._partition._release(self._name)

    async def record_success(self) -> None:
        await self._partition.record_success()

    async def record_throttle(self, *, retry_after: float | None = None) -> None:
        await self._partition.record_throttle(retry_after=retry_after)
