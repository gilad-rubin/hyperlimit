"""Retry-After cooldowns, lane-wide pause on throttle, and cooldown jitter."""

from __future__ import annotations

import asyncio

from hyperlimit import AdaptiveLimiter

from tests._fake_time import FakeTime, drain


def _limiter(fake: FakeTime, **kwargs) -> AdaptiveLimiter:
    defaults = dict(
        initial=8,
        floor=1,
        cap=12,
        cooldown_seconds=30.0,
        clock=fake.clock,
        sleep=fake.sleep,
    )
    defaults.update(kwargs)
    return AdaptiveLimiter(**defaults)


########## Retry-After as the cooldown ##########


def test_sane_retry_after_sets_the_cooldown_window() -> None:
    fake = FakeTime()
    limiter = _limiter(fake)

    async def main() -> None:
        await limiter.record_throttle(retry_after=10.0)  # 8 -> 4, window 10s
        fake.advance(11)
        await limiter.record_throttle()  # past the 10s window: cuts again
        await drain()

    asyncio.run(main())
    assert limiter.limit == 2


def test_insane_retry_after_falls_back_to_fixed_cooldown() -> None:
    fake = FakeTime()
    limiter = _limiter(fake, max_retry_after=60.0)

    async def main() -> None:
        await limiter.record_throttle(retry_after=3600.0)  # ignored: > max
        fake.advance(31)
        await limiter.record_throttle(retry_after=-2.0)  # ignored: not positive
        await drain()

    asyncio.run(main())
    assert limiter.limit == 2  # both cuts landed on the fixed 30s window


def test_retry_after_within_window_is_absorbed() -> None:
    fake = FakeTime()
    limiter = _limiter(fake)

    async def main() -> None:
        await limiter.record_throttle(retry_after=20.0)  # 8 -> 4
        fake.advance(5)
        await limiter.record_throttle(retry_after=1.0)  # inside the 20s window
        await drain()

    asyncio.run(main())
    assert limiter.limit == 4


########## lane-wide pause on throttle ##########


def test_throttle_pauses_new_admissions_until_the_window_ends() -> None:
    fake = FakeTime()
    limiter = _limiter(fake)
    admitted: list[str] = []

    async def main() -> None:
        await limiter.record_throttle(retry_after=10.0)

        async def enter() -> None:
            async with limiter:
                admitted.append("in")

        task = asyncio.create_task(enter())
        await drain()
        assert admitted == []  # paused: no new admissions
        fake.advance(10.0)
        await drain()
        assert admitted == ["in"]  # waker fired, waiter admitted
        await task

    asyncio.run(main())


def test_pause_does_not_evict_permits_already_held() -> None:
    fake = FakeTime()
    limiter = _limiter(fake)

    async def main() -> None:
        await limiter.acquire()
        await limiter.record_throttle()
        assert limiter.active == 1  # held work keeps running
        await limiter.release()

    asyncio.run(main())


def test_pause_can_be_disabled() -> None:
    fake = FakeTime()
    limiter = _limiter(fake, pause_on_throttle=False)
    admitted: list[str] = []

    async def main() -> None:
        await limiter.record_throttle()

        async def enter() -> None:
            async with limiter:
                admitted.append("in")

        task = asyncio.create_task(enter())
        await drain()
        await task

    asyncio.run(main())
    assert admitted == ["in"]


def test_snapshot_reports_pause_state() -> None:
    fake = FakeTime()
    limiter = _limiter(fake)

    async def main() -> None:
        assert limiter.snapshot()["paused"] is False
        await limiter.record_throttle(retry_after=10.0)
        assert limiter.snapshot()["paused"] is True
        fake.advance(10.0)
        await drain()
        assert limiter.snapshot()["paused"] is False

    asyncio.run(main())


########## jitter on the cooldown window ##########


def test_jitter_extends_the_window_via_injected_rng() -> None:
    fake = FakeTime()
    limiter = _limiter(fake, jitter=0.5, rng=lambda: 1.0)  # window * 1.5

    async def main() -> None:
        await limiter.record_throttle(retry_after=10.0)  # window = 15s
        fake.advance(11)
        await limiter.record_throttle()  # still inside the jittered window
        fake.advance(5)
        await limiter.record_throttle()  # past it: cuts again
        await drain()

    asyncio.run(main())
    assert limiter.limit == 2


def test_zero_jitter_is_the_default_and_changes_nothing() -> None:
    fake = FakeTime()
    limiter = _limiter(fake)

    async def main() -> None:
        await limiter.record_throttle(retry_after=10.0)
        await drain()
        assert fake.pending_sleeps == [10.0]

    asyncio.run(main())
