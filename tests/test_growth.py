"""Growth modes: fixed +1 versus sqrt-limit-proportional increase."""

from __future__ import annotations

import asyncio

import pytest

from hyperlimit import AdaptiveLimiter, PartitionedLimiter


def test_sqrt_growth_scales_with_the_current_limit() -> None:
    limiter = AdaptiveLimiter(
        initial=4, floor=1, cap=100, successes_per_increase=2, growth="sqrt", growth_threshold=0.0
    )

    async def main() -> None:
        await limiter.record_success()
        await limiter.record_success()  # 4 + max(1, int(sqrt(4))) = 6
        assert limiter.limit == 6
        await limiter.record_success()
        await limiter.record_success()  # 6 + int(sqrt(6)) = 8
        assert limiter.limit == 8

    asyncio.run(main())


def test_sqrt_growth_still_respects_the_cap() -> None:
    limiter = AdaptiveLimiter(
        initial=9, floor=1, cap=10, successes_per_increase=1, growth="sqrt", growth_threshold=0.0
    )

    async def main() -> None:
        await limiter.record_success()  # 9 + 3 clamps to 10

    asyncio.run(main())
    assert limiter.limit == 10


def test_fixed_growth_remains_the_default() -> None:
    limiter = AdaptiveLimiter(
        initial=4, floor=1, cap=8, successes_per_increase=1, growth_threshold=0.0
    )

    async def main() -> None:
        await limiter.record_success()

    asyncio.run(main())
    assert limiter.limit == 5


def test_unknown_growth_mode_is_rejected() -> None:
    with pytest.raises(ValueError):
        AdaptiveLimiter(growth="vegas")


########## growth needs load (growth_threshold) ##########


def test_idle_successes_never_raise_the_limit() -> None:
    limiter = AdaptiveLimiter(initial=4, floor=1, cap=16, successes_per_increase=1)

    async def main() -> None:
        for _ in range(1000):
            await limiter.record_success()

    asyncio.run(main())
    assert limiter.limit == 4


def test_one_call_at_a_time_does_not_creep_toward_the_cap() -> None:
    # The transport's pattern: the verdict lands while the permit is held.
    limiter = AdaptiveLimiter(initial=4, floor=1, cap=16, successes_per_increase=1)

    async def main() -> None:
        for _ in range(1000):
            async with limiter:
                await limiter.record_success()  # 1 of 4 in use: under half

    asyncio.run(main())
    assert limiter.limit == 4


def test_successes_under_load_still_grow_the_limit() -> None:
    limiter = AdaptiveLimiter(initial=4, floor=1, cap=16, successes_per_increase=1)

    async def main() -> None:
        await limiter.acquire()
        await limiter.acquire()  # 2 of 4 in use: half the limit
        await limiter.record_success()
        assert limiter.limit == 5
        await limiter.release()
        await limiter.release()

    asyncio.run(main())


def test_waiting_callers_count_as_load() -> None:
    # The README's pattern: the verdict lands after the permit is released,
    # while a queued caller has not yet run.
    limiter = AdaptiveLimiter(initial=1, floor=1, cap=4, successes_per_increase=1)

    async def main() -> None:
        await limiter.acquire()
        waiter = asyncio.create_task(limiter.acquire())
        await asyncio.sleep(0)
        assert limiter.waiting == 1
        await limiter.release()
        await limiter.record_success()  # demand: the parked waiter
        assert limiter.limit == 2
        await waiter
        await limiter.release()

    asyncio.run(main())


def test_growth_threshold_is_validated() -> None:
    with pytest.raises(ValueError):
        AdaptiveLimiter(growth_threshold=1.5)
    with pytest.raises(ValueError):
        AdaptiveLimiter(growth_threshold=-0.1)


def test_lane_waiters_are_load_on_the_shared_limit() -> None:
    parent = AdaptiveLimiter(initial=4, floor=1, cap=16, successes_per_increase=1)
    part = PartitionedLimiter(parent, shares={"a": 0.25, "b": 0.75})  # a's reserve: 1
    lane = part.lane("a")

    async def main() -> None:
        await lane.acquire()
        waiter = asyncio.create_task(lane.acquire())  # parked on a's reserve
        await asyncio.sleep(0)
        assert parent.waiting == 1
        assert part.snapshot()["lanes"]["a"]["waiting"] == 1
        await lane.record_success()  # 1 held + 1 waiting = half of 4
        assert parent.limit == 5
        await lane.release()
        await waiter
        await lane.release()

    asyncio.run(main())
