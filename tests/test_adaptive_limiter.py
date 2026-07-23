"""AdaptiveLimiter: AIMD behavior, permit bounds, cooldown, wake-ups."""

from __future__ import annotations

import asyncio

import pytest

from hyperlimit import AdaptiveLimiter


def test_rejects_inconsistent_bounds() -> None:
    with pytest.raises(ValueError):
        AdaptiveLimiter(initial=1, floor=2, cap=4)
    with pytest.raises(ValueError):
        AdaptiveLimiter(initial=8, floor=1, cap=4)
    with pytest.raises(ValueError):
        AdaptiveLimiter(decrease_factor=1.0)


def test_concurrency_never_exceeds_limit() -> None:
    limiter = AdaptiveLimiter(initial=3, floor=1, cap=8)
    tracker = {"now": 0, "peak": 0}

    async def worker() -> None:
        async with limiter:
            tracker["now"] += 1
            tracker["peak"] = max(tracker["peak"], tracker["now"])
            await asyncio.sleep(0.003)
            tracker["now"] -= 1

    async def main() -> None:
        await asyncio.gather(*(worker() for _ in range(12)))

    asyncio.run(main())
    assert tracker["peak"] <= 3
    assert tracker["peak"] >= 2


def test_additive_increase_after_n_successes() -> None:
    changes: list[tuple[int, int, str]] = []
    limiter = AdaptiveLimiter(
        initial=2, floor=1, cap=4, successes_per_increase=3, on_change=lambda *a: changes.append(a)
    )

    async def main() -> None:
        for _ in range(6):
            await limiter.record_success()

    asyncio.run(main())
    assert limiter.limit == 4
    assert changes == [(2, 3, "additive-increase"), (3, 4, "additive-increase")]


def test_increase_stops_at_cap() -> None:
    limiter = AdaptiveLimiter(initial=3, floor=1, cap=3, successes_per_increase=1)

    async def main() -> None:
        for _ in range(5):
            await limiter.record_success()

    asyncio.run(main())
    assert limiter.limit == 3


def test_throttle_halves_and_respects_floor() -> None:
    clock = {"now": 0.0}
    limiter = AdaptiveLimiter(
        initial=9, floor=2, cap=12, cooldown_seconds=30.0, clock=lambda: clock["now"]
    )

    async def main() -> None:
        await limiter.record_throttle()  # 9 -> 4
        clock["now"] += 31
        await limiter.record_throttle()  # 4 -> 2
        clock["now"] += 31
        await limiter.record_throttle()  # floor holds

    asyncio.run(main())
    assert limiter.limit == 2


def test_cooldown_absorbs_a_burst_into_one_cut() -> None:
    clock = {"now": 100.0}
    limiter = AdaptiveLimiter(
        initial=8, floor=1, cap=12, cooldown_seconds=60.0, clock=lambda: clock["now"]
    )

    async def main() -> None:
        await limiter.record_throttle()  # 8 -> 4
        clock["now"] += 5
        await limiter.record_throttle()  # within cooldown: absorbed
        await limiter.record_throttle()  # absorbed
        clock["now"] += 61
        await limiter.record_throttle()  # new window: 4 -> 2

    asyncio.run(main())
    assert limiter.limit == 2


def test_throttle_resets_success_progress() -> None:
    clock = {"now": 0.0}
    limiter = AdaptiveLimiter(
        initial=4, floor=1, cap=8, successes_per_increase=3, clock=lambda: clock["now"]
    )

    async def main() -> None:
        await limiter.record_success()
        await limiter.record_success()
        await limiter.record_throttle()  # 4 -> 2, progress cleared
        await limiter.record_success()   # 1 of 3 — no increase yet

    asyncio.run(main())
    assert limiter.limit == 2


def test_increase_wakes_blocked_waiters() -> None:
    limiter = AdaptiveLimiter(initial=1, floor=1, cap=2, successes_per_increase=1)
    order: list[str] = []

    async def holder() -> None:
        async with limiter:
            order.append("holder-in")
            await limiter.record_success()  # limit 1 -> 2 wakes the waiter
            await asyncio.sleep(0.02)
            order.append("holder-out")

    async def waiter() -> None:
        await asyncio.sleep(0.005)  # ensure holder owns the only permit first
        async with limiter:
            order.append("waiter-in")

    async def main() -> None:
        await asyncio.gather(holder(), waiter())

    asyncio.run(main())
    assert order == ["holder-in", "waiter-in", "holder-out"]
