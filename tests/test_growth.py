"""Growth modes: fixed +1 versus sqrt-limit-proportional increase."""

from __future__ import annotations

import asyncio

import pytest

from hyperlimit import AdaptiveLimiter


def test_sqrt_growth_scales_with_the_current_limit() -> None:
    limiter = AdaptiveLimiter(
        initial=4, floor=1, cap=100, successes_per_increase=2, growth="sqrt"
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
        initial=9, floor=1, cap=10, successes_per_increase=1, growth="sqrt"
    )

    async def main() -> None:
        await limiter.record_success()  # 9 + 3 clamps to 10

    asyncio.run(main())
    assert limiter.limit == 10


def test_fixed_growth_remains_the_default() -> None:
    limiter = AdaptiveLimiter(initial=4, floor=1, cap=8, successes_per_increase=1)

    async def main() -> None:
        await limiter.record_success()

    asyncio.run(main())
    assert limiter.limit == 5


def test_unknown_growth_mode_is_rejected() -> None:
    with pytest.raises(ValueError):
        AdaptiveLimiter(growth="vegas")
