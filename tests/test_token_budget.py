"""TokenBudget: a continuous-refill tokens-per-minute bucket beside the permits."""

from __future__ import annotations

import asyncio

import pytest

from hyperlimit import TokenBudget

from tests._fake_time import FakeTime, drain


def test_spend_within_budget_is_immediate() -> None:
    fake = FakeTime()
    budget = TokenBudget(6000, clock=fake.clock, sleep=fake.sleep)

    async def main() -> None:
        await budget.spend(1000)
        await budget.spend(2000)

    asyncio.run(main())
    assert budget.available == 3000


def test_spend_waits_for_refill_when_the_bucket_runs_dry() -> None:
    fake = FakeTime()
    budget = TokenBudget(6000, clock=fake.clock, sleep=fake.sleep)  # 100 tokens/s
    done: list[str] = []

    async def main() -> None:
        await budget.spend(6000)  # bucket empty

        async def spender() -> None:
            await budget.spend(500)
            done.append("spent")

        task = asyncio.create_task(spender())
        await drain()
        assert done == []
        fake.advance(5.0)  # refills exactly 500
        await drain()
        assert done == ["spent"]
        await task

    asyncio.run(main())


def test_refill_never_exceeds_capacity() -> None:
    fake = FakeTime()
    budget = TokenBudget(6000, clock=fake.clock, sleep=fake.sleep)

    async def main() -> None:
        await budget.spend(1000)
        fake.advance(3600)
        await budget.spend(0)  # touch to refill

    asyncio.run(main())
    assert budget.available == 6000


def test_oversized_request_admits_at_full_bucket_and_debts_forward() -> None:
    fake = FakeTime()
    budget = TokenBudget(6000, clock=fake.clock, sleep=fake.sleep)
    done: list[str] = []

    async def main() -> None:
        await budget.spend(9000)  # bigger than a full bucket: admitted, debt -3000
        assert budget.available == -3000

        async def spender() -> None:
            await budget.spend(100)
            done.append("spent")

        task = asyncio.create_task(spender())
        await drain()
        assert done == []  # the debt delays followers
        fake.advance(31.0)  # 3000 debt + 100 tokens at 100/s
        await drain()
        assert done == ["spent"]
        await task

    asyncio.run(main())


def test_adjust_reconciles_estimate_with_actual_usage() -> None:
    fake = FakeTime()
    budget = TokenBudget(6000, clock=fake.clock, sleep=fake.sleep)

    async def main() -> None:
        await budget.spend(2000)  # estimated
        budget.adjust(500)  # actual was 1500: refund the difference

    asyncio.run(main())
    assert budget.available == 4500


def test_rejects_a_nonpositive_rate() -> None:
    with pytest.raises(ValueError):
        TokenBudget(0)
