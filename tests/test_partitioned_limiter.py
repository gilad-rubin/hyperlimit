"""PartitionedLimiter: lanes sharing one quota partition ONE AIMD loop.

Two independent adaptive limiters on one provider quota fight — one lane's
throttle teaches the other nothing, so it keeps pushing into the same 429
storm. Partitions share the limit and the verdicts.
"""

from __future__ import annotations

import asyncio

import pytest

from hyperlimit import AdaptiveLimiter, PartitionedLimiter

from tests._fake_time import FakeTime, drain


def _partitioned(fake: FakeTime, **limiter_kwargs) -> PartitionedLimiter:
    defaults = dict(
        initial=10,
        floor=2,
        cap=20,
        cooldown_seconds=30.0,
        clock=fake.clock,
        sleep=fake.sleep,
    )
    defaults.update(limiter_kwargs)
    return PartitionedLimiter(
        AdaptiveLimiter(**defaults), shares={"chat": 0.8, "embed": 0.2}
    )


def test_reserved_shares_derive_from_the_live_limit() -> None:
    fake = FakeTime()
    part = _partitioned(fake)
    assert part.reserved("chat") == 8
    assert part.reserved("embed") == 2


def test_a_lane_cannot_exceed_its_reserve_even_with_parent_headroom() -> None:
    fake = FakeTime()
    part = _partitioned(fake)
    embed = part.lane("embed")
    admitted: list[int] = []
    hold = asyncio.Event()

    async def main() -> None:
        async def enter(i: int) -> None:
            async with embed:
                admitted.append(i)
                await hold.wait()

        tasks = [asyncio.create_task(enter(i)) for i in range(5)]
        await drain()
        assert len(admitted) == 2  # reserve is 2 of 10, headroom notwithstanding
        hold.set()
        await drain()
        await asyncio.gather(*tasks)

    asyncio.run(main())


def test_lanes_together_never_exceed_the_parent_limit() -> None:
    fake = FakeTime()
    part = _partitioned(fake, initial=4, floor=2, cap=20)
    # reserves round up: chat 3 + embed 1 = 4 == limit; parent still caps totals
    tracker = {"now": 0, "peak": 0}

    async def main() -> None:
        async def enter(lane_name: str) -> None:
            async with part.lane(lane_name):
                tracker["now"] += 1
                tracker["peak"] = max(tracker["peak"], tracker["now"])
                await asyncio.sleep(0)
                tracker["now"] -= 1

        await asyncio.gather(
            *(enter("chat") for _ in range(6)), *(enter("embed") for _ in range(6))
        )

    asyncio.run(main())
    assert tracker["peak"] <= 4


def test_one_lane_throttle_shrinks_and_pauses_every_lane() -> None:
    fake = FakeTime()
    part = _partitioned(fake)
    chat, embed = part.lane("chat"), part.lane("embed")
    admitted: list[str] = []

    async def main() -> None:
        await embed.record_throttle(retry_after=10.0)  # one loop: 10 -> 5
        assert part.limit == 5
        assert part.reserved("chat") == 4

        async def enter() -> None:
            async with chat:
                admitted.append("chat")

        task = asyncio.create_task(enter())
        await drain()
        assert admitted == []  # the shared pause blocks the OTHER lane too
        fake.advance(10.0)
        await drain()
        assert admitted == ["chat"]
        await task

    asyncio.run(main())


def test_growth_from_one_lane_wakes_waiters_in_the_grown_reserve() -> None:
    fake = FakeTime()
    part = _partitioned(fake, initial=5, floor=1, cap=20, successes_per_increase=1)
    embed = part.lane("embed")  # reserve at limit 5: max(1, round(1.0)) = 1
    admitted: list[int] = []

    async def main() -> None:
        await embed.acquire()  # reserve full

        async def enter() -> None:
            async with embed:
                admitted.append(1)

        task = asyncio.create_task(enter())
        await drain()
        assert admitted == []
        for _ in range(5):
            await embed.record_success()  # limit 5 -> 10, embed reserve 1 -> 2
        await drain()
        assert admitted == [1]
        await task
        await embed.release()

    asyncio.run(main())


def test_share_validation() -> None:
    limiter = AdaptiveLimiter(initial=4, floor=1, cap=8)
    with pytest.raises(ValueError):
        PartitionedLimiter(limiter, shares={})
    with pytest.raises(ValueError):
        PartitionedLimiter(limiter, shares={"a": 0.0, "b": 1.0})
    with pytest.raises(ValueError):
        PartitionedLimiter(limiter, shares={"a": 0.7, "b": 0.7})
    part = PartitionedLimiter(limiter, shares={"a": 0.5, "b": 0.5})
    with pytest.raises(KeyError):
        part.lane("missing")
