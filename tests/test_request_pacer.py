"""RequestPacer: deterministic requests-per-second admission, not concurrency."""

from __future__ import annotations

import asyncio

import pytest

from hyperlimit import RequestPacer
from tests._fake_time import FakeTime, drain


def test_rejects_a_nonpositive_rate() -> None:
    with pytest.raises(ValueError):
        RequestPacer(0)


def test_first_request_is_immediate_and_followers_are_evenly_paced() -> None:
    fake = FakeTime()
    pacer = RequestPacer(4, clock=fake.clock, sleep=fake.sleep)
    admitted: list[float] = []

    async def request() -> None:
        await pacer.acquire()
        admitted.append(fake.clock())

    async def main() -> None:
        tasks = [asyncio.create_task(request()) for _ in range(3)]
        await drain()
        assert admitted == [0.0]
        assert fake.pending_sleeps == pytest.approx([0.25, 0.5])

        fake.advance(fake.pending_sleeps[0] - fake.clock())
        await drain()
        assert admitted == pytest.approx([0.0, 0.25])

        fake.advance(fake.pending_sleeps[0] - fake.clock())
        await drain()
        assert admitted == pytest.approx([0.0, 0.25, 0.5])
        await asyncio.gather(*tasks)

    asyncio.run(main())


def test_idle_time_refills_one_immediate_request_not_a_burst() -> None:
    fake = FakeTime()
    pacer = RequestPacer(2, clock=fake.clock, sleep=fake.sleep)
    admitted: list[float] = []

    async def request() -> None:
        await pacer.acquire()
        admitted.append(fake.clock())

    async def main() -> None:
        await request()
        fake.advance(10)
        first = asyncio.create_task(request())
        second = asyncio.create_task(request())
        await drain()

        assert admitted == [0.0, 10.0]
        assert fake.pending_sleeps == pytest.approx([10.5])

        fake.advance(fake.pending_sleeps[0] - fake.clock())
        await drain()
        await asyncio.gather(first, second)

    asyncio.run(main())
    assert admitted == pytest.approx([0.0, 10.0, 10.5])


def test_concurrent_admission_never_exceeds_rate_in_a_rolling_second() -> None:
    fake = FakeTime()
    quota = 5
    pacer = RequestPacer(quota, clock=fake.clock, sleep=fake.sleep)
    admitted: list[float] = []

    async def request() -> None:
        await pacer.acquire()
        admitted.append(fake.clock())

    async def main() -> None:
        tasks = [asyncio.create_task(request()) for _ in range(20)]
        await drain()
        for _ in range(19):
            fake.advance(fake.pending_sleeps[0] - fake.clock())
            await drain()
        await asyncio.gather(*tasks)

    asyncio.run(main())

    assert len(admitted) == 20
    assert all(
        sum(start <= timestamp < start + 1 for timestamp in admitted) <= quota
        for start in admitted
    )
