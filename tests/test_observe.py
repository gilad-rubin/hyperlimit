"""observe_limits: scoped telemetry for admissions, releases and limit moves."""

from __future__ import annotations

import asyncio
import logging

import pytest

from hyperlimit import (
    AdaptiveLimiter,
    AdmissionTimeout,
    Admitted,
    LimitChanged,
    PartitionedLimiter,
    Released,
    Throttled,
    TimedOut,
    observe_limits,
)

from tests._fake_time import FakeTime, drain


def test_an_uncontended_admission_and_its_release() -> None:
    events: list[object] = []
    limiter = AdaptiveLimiter(initial=3, floor=1, cap=8, name="parse", clock=FakeTime().clock)

    async def main() -> None:
        with observe_limits(events.append):
            async with limiter:
                pass

    asyncio.run(main())
    assert events == [
        Admitted(limiter="parse", lane=None, waited=0.0, queued=0, limit=3, active=1),
        Released(limiter="parse", lane=None, limit=3, active=0, waiting=0),
    ]


def test_a_contended_admission_reports_its_wait_and_queue() -> None:
    fake = FakeTime()
    events: list[object] = []
    limiter = AdaptiveLimiter(initial=1, floor=1, cap=4, clock=fake.clock, sleep=fake.sleep)

    async def main() -> None:
        with observe_limits(events.append):
            await limiter.acquire()
            first = asyncio.create_task(limiter.acquire())
            await drain()
            second = asyncio.create_task(limiter.acquire())
            await drain()
            fake.advance(4.0)
            await limiter.release()
            await drain()
            await first
            fake.advance(1.0)
            await limiter.release()
            await drain()
            await second

    asyncio.run(main())
    admitted = [event for event in events if isinstance(event, Admitted)]
    assert [(event.waited, event.queued) for event in admitted] == [(0.0, 0), (4.0, 0), (5.0, 1)]


def test_a_timeout_is_reported_before_it_is_raised() -> None:
    fake = FakeTime()
    events: list[object] = []
    limiter = AdaptiveLimiter(
        initial=1, floor=1, cap=4, max_wait=2.0, clock=fake.clock, sleep=fake.sleep
    )

    async def main() -> None:
        with observe_limits(events.append):
            await limiter.acquire()
            waiter = asyncio.create_task(limiter.acquire())
            await drain()
            fake.advance(2.0)
            await drain()
            with pytest.raises(AdmissionTimeout):
                await waiter

    asyncio.run(main())
    assert events[-1] == TimedOut(
        limiter=None, lane=None, waited=2.0, queued=0, limit=1, active=1
    )


def test_throttles_and_limit_moves() -> None:
    fake = FakeTime()
    events: list[object] = []
    limiter = AdaptiveLimiter(
        initial=8, floor=1, cap=12, cooldown_seconds=30.0, name="model",
        clock=fake.clock, sleep=fake.sleep,
    )

    async def main() -> None:
        with observe_limits(events.append):
            await limiter.record_throttle(retry_after=10.0)
            await limiter.record_throttle()  # inside the window: absorbed
            await drain()

    asyncio.run(main())
    assert events == [
        LimitChanged(limiter="model", old=8, new=4, reason="multiplicative-decrease"),
        Throttled(limiter="model", retry_after=10.0, window=10.0, limit=4),
        Throttled(limiter="model", retry_after=None, window=None, limit=4),
    ]


def test_a_lane_admission_is_one_event_named_by_its_lane() -> None:
    events: list[object] = []
    part = PartitionedLimiter(
        AdaptiveLimiter(initial=4, floor=2, cap=8, name="openai", clock=FakeTime().clock),
        shares={"chat": 0.5, "batch": 0.5},
    )

    async def main() -> None:
        with observe_limits(events.append):
            async with part.lane("chat"):
                pass

    asyncio.run(main())
    assert events == [
        Admitted(limiter="openai", lane="chat", waited=0.0, queued=0, limit=4, active=1),
        Released(limiter="openai", lane="chat", limit=4, active=0, waiting=0),
    ]


def test_nothing_is_reported_outside_the_scope() -> None:
    events: list[object] = []
    limiter = AdaptiveLimiter()

    async def main() -> None:
        async with limiter:
            pass
        with observe_limits(events.append):
            with observe_limits(lambda event: None):
                async with limiter:
                    pass  # the inner observer takes these
            async with limiter:
                pass  # the outer one is back

    asyncio.run(main())
    assert [type(event) for event in events] == [Admitted, Released]


def test_a_failing_observer_never_changes_admission(caplog: pytest.LogCaptureFixture) -> None:
    def broken(event: object) -> None:
        raise RuntimeError("observer bug")

    limiter = AdaptiveLimiter()

    async def main() -> None:
        with observe_limits(broken):
            async with limiter:
                assert limiter.active == 1

    with caplog.at_level(logging.WARNING, logger="hyperlimit._observe"):
        asyncio.run(main())
    assert limiter.active == 0
    assert "observer raised" in caplog.text
