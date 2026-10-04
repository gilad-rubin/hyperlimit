"""Bounded admission: `max_wait` turns an unbounded wait into AdmissionTimeout."""

from __future__ import annotations

import asyncio
import pickle

import pytest

from hyperlimit import AdaptiveLimiter, AdmissionTimeout, PartitionedLimiter

from tests._fake_time import FakeTime, drain


def _limiter(fake: FakeTime, **kwargs) -> AdaptiveLimiter:
    defaults = dict(initial=1, floor=1, cap=4, clock=fake.clock, sleep=fake.sleep)
    defaults.update(kwargs)
    return AdaptiveLimiter(**defaults)


def test_a_full_limiter_times_out_at_max_wait_and_holds_nothing() -> None:
    fake = FakeTime()
    limiter = _limiter(fake, max_wait=5.0, name="parse")

    async def main() -> None:
        await limiter.acquire()
        waiter = asyncio.create_task(limiter.acquire())
        await drain()
        fake.advance(4.9)
        await drain()
        assert not waiter.done()
        fake.advance(0.1)
        await drain()
        assert waiter.done()  # timed out, not still parked
        with pytest.raises(AdmissionTimeout) as caught:
            await waiter
        error = caught.value
        assert (error.waited, error.queued, error.limit) == (5.0, 0, 1)
        assert (error.limiter, error.lane) == ("parse", None)
        assert isinstance(error, TimeoutError)
        assert "parse" in str(error)
        assert (limiter.active, limiter.waiting) == (1, 0)

    asyncio.run(main())


def test_an_admission_before_the_deadline_cancels_the_timeout() -> None:
    fake = FakeTime()
    limiter = _limiter(fake, max_wait=5.0)

    async def main() -> None:
        await limiter.acquire()
        waiter = asyncio.create_task(limiter.acquire())
        await drain()
        fake.advance(2.0)
        await limiter.release()
        await drain()
        await waiter  # admitted, no exception
        fake.advance(10.0)  # well past the old deadline
        await drain()
        assert (limiter.active, limiter.waiting) == (1, 0)

    asyncio.run(main())


def test_per_call_max_wait_overrides_the_constructor() -> None:
    fake = FakeTime()
    limiter = _limiter(fake)  # unbounded by default

    async def main() -> None:
        await limiter.acquire()
        waiter = asyncio.create_task(limiter.acquire(max_wait=1.0))
        await drain()
        fake.advance(1.0)
        await drain()
        assert waiter.done()  # timed out, not still parked
        with pytest.raises(AdmissionTimeout):
            await waiter

    asyncio.run(main())


def test_a_throttle_pause_times_out_too() -> None:
    fake = FakeTime()
    limiter = _limiter(fake, initial=4, max_wait=5.0)

    async def main() -> None:
        await limiter.record_throttle(retry_after=30.0)  # paused for 30 s
        waiter = asyncio.create_task(limiter.acquire())
        await drain()
        fake.advance(5.0)
        await drain()
        assert waiter.done()  # timed out, not still parked
        with pytest.raises(AdmissionTimeout):
            await waiter
        assert limiter.active == 0

    asyncio.run(main())


def test_queued_counts_the_callers_already_waiting() -> None:
    fake = FakeTime()
    limiter = _limiter(fake, max_wait=5.0)

    async def main() -> None:
        await limiter.acquire()
        first = asyncio.create_task(limiter.acquire())
        await drain()
        second = asyncio.create_task(limiter.acquire())
        await drain()
        assert limiter.waiting == 2
        fake.advance(5.0)
        await drain()
        assert first.done() and second.done()
        results = await asyncio.gather(first, second, return_exceptions=True)
        assert [error.queued for error in results] == [0, 1]
        assert limiter.waiting == 0

    asyncio.run(main())


def test_admission_timeout_survives_pickling() -> None:
    error = AdmissionTimeout(waited=2.5, queued=3, limit=4, limiter="parse", lane="chat")
    copy = pickle.loads(pickle.dumps(error))
    assert (copy.waited, copy.queued, copy.limit, copy.limiter, copy.lane) == (
        2.5, 3, 4, "parse", "chat",
    )
    assert str(copy) == str(error)


def test_max_wait_must_be_positive() -> None:
    with pytest.raises(ValueError):
        AdaptiveLimiter(max_wait=0)

    async def main() -> None:
        with pytest.raises(ValueError):
            await AdaptiveLimiter().acquire(max_wait=-1.0)

    asyncio.run(main())


########## lanes ##########


def _partitioned(fake: FakeTime, **kwargs) -> PartitionedLimiter:
    defaults = dict(initial=2, floor=1, cap=4, clock=fake.clock, sleep=fake.sleep)
    defaults.update(kwargs)
    return PartitionedLimiter(AdaptiveLimiter(**defaults), shares={"a": 0.5, "b": 0.5})


def test_a_lane_wait_for_its_reserve_times_out() -> None:
    fake = FakeTime()
    part = _partitioned(fake, max_wait=3.0)  # each lane's reserve: 1
    lane = part.lane("a")

    async def main() -> None:
        await lane.acquire()
        waiter = asyncio.create_task(lane.acquire())
        await drain()
        fake.advance(3.0)
        await drain()
        assert waiter.done()  # timed out, not still parked
        with pytest.raises(AdmissionTimeout) as caught:
            await waiter
        assert caught.value.lane == "a"
        lanes = part.snapshot()["lanes"]
        assert (lanes["a"]["active"], lanes["a"]["waiting"]) == (1, 0)
        assert part.snapshot()["waiting"] == 0

    asyncio.run(main())


def test_one_deadline_covers_the_reserve_and_the_shared_permit() -> None:
    fake = FakeTime()
    part = _partitioned(fake)
    lane = part.lane("a")

    async def main() -> None:
        await lane.record_throttle(retry_after=30.0)  # the shared limit pauses
        waiter = asyncio.create_task(lane.acquire(max_wait=4.0))
        await drain()
        fake.advance(4.0)
        await drain()
        assert waiter.done()  # timed out, not still parked
        with pytest.raises(AdmissionTimeout):
            await waiter
        # the reserve slot taken on the way in is handed back
        assert part.snapshot()["lanes"]["a"]["active"] == 0
        assert part.snapshot()["active"] == 0

    asyncio.run(main())
