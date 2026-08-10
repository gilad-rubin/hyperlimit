"""LimitedTransport: one permit per HTTP attempt, verdicts from the wire.

Tests run over `httpx.MockTransport` — the network is fake, everything above
it (status handling, the limiter) is real.
"""

from __future__ import annotations

import asyncio
import subprocess
import sys
from types import SimpleNamespace
from typing import Any, Callable

import httpx
import pytest

from hyperlimit import AdaptiveLimiter, PartitionedLimiter
from hyperlimit.httpx import Limiter, LimitedTransport, governing_limiter, retry_after_from


class RecordingLimiter:
    """A duck limiter: plain recording implementation, not an AdaptiveLimiter."""

    def __init__(self) -> None:
        self.active = 0
        self.peak = 0
        self.successes = 0
        self.throttles: list[float | None] = []

    async def __aenter__(self) -> "RecordingLimiter":
        self.active += 1
        self.peak = max(self.peak, self.active)
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        self.active -= 1

    async def record_success(self) -> None:
        self.successes += 1

    async def record_throttle(self, *, retry_after: float | None = None) -> None:
        self.throttles.append(retry_after)


def limited_client(
    limiter: Limiter, handler: Callable[[httpx.Request], httpx.Response]
) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=LimitedTransport(limiter=limiter, transport=httpx.MockTransport(handler))
    )


# ── the permit ──────────────────────────────────────────────────────────


def test_permit_covers_exactly_one_attempt() -> None:
    limiter = AdaptiveLimiter(initial=3, floor=1, cap=8)
    seen_active: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_active.append(limiter.active)
        return httpx.Response(200)

    async def main() -> None:
        async with limited_client(limiter, handler) as client:
            await client.get("http://test/one")
            assert limiter.active == 0  # released between attempts
            await client.get("http://test/two")
            assert limiter.active == 0

    asyncio.run(main())
    assert seen_active == [1, 1]  # held during each attempt


def test_transport_raise_releases_permit_without_verdict() -> None:
    limiter = RecordingLimiter()

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("wire down")

    async def main() -> None:
        async with limited_client(limiter, handler) as client:
            with pytest.raises(httpx.ConnectError):
                await client.get("http://test/")

    asyncio.run(main())
    assert limiter.active == 0
    assert limiter.successes == 0
    assert limiter.throttles == []


# ── the verdicts ────────────────────────────────────────────────────────


def test_every_429_records_a_throttle_with_parsed_retry_after() -> None:
    limiter = RecordingLimiter()
    responses = [
        httpx.Response(429, headers={"retry-after-ms": "1500", "retry-after": "9"}),
        httpx.Response(429, headers={"retry-after": "2"}),
        httpx.Response(429),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    async def main() -> None:
        async with limited_client(limiter, handler) as client:
            for _ in range(3):
                await client.get("http://test/")

    asyncio.run(main())
    assert limiter.throttles == [1.5, 2.0, None]  # ms wins over seconds
    assert limiter.successes == 0


def test_success_below_400_and_silence_on_client_and_server_errors() -> None:
    limiter = RecordingLimiter()
    responses = [
        httpx.Response(200),
        httpx.Response(302),
        httpx.Response(400),
        httpx.Response(500),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    async def main() -> None:
        async with limited_client(limiter, handler) as client:
            for _ in range(4):
                await client.get("http://test/")

    asyncio.run(main())
    assert limiter.successes == 2  # 200 and 302
    assert limiter.throttles == []  # 400/500 teach nothing
    assert limiter.active == 0


# ── retry_after_from ────────────────────────────────────────────────────


def test_retry_after_from_ms_takes_precedence() -> None:
    headers = httpx.Headers({"retry-after-ms": "250", "retry-after": "7"})
    assert retry_after_from(headers) == 0.25


def test_retry_after_from_seconds() -> None:
    assert retry_after_from(httpx.Headers({"retry-after": "3"})) == 3.0


def test_retry_after_from_http_date_and_garbage_yield_none() -> None:
    assert retry_after_from(httpx.Headers({"retry-after": "Wed, 21 Oct 2015 07:28:00 GMT"})) is None
    assert retry_after_from(httpx.Headers({"retry-after-ms": "soon"})) is None
    assert retry_after_from(None) is None
    assert retry_after_from(httpx.Headers()) is None


# ── governing_limiter ───────────────────────────────────────────────────


def test_governing_limiter_finds_the_limiter_through_a_wrapper_chain() -> None:
    limiter = AdaptiveLimiter(initial=2, floor=1, cap=4)
    client = httpx.AsyncClient(transport=LimitedTransport(limiter=limiter))
    sdk_like = SimpleNamespace(_client=client)
    proxy = SimpleNamespace(_client=sdk_like)

    assert governing_limiter(client) is limiter
    assert governing_limiter(proxy) is limiter


def test_governing_limiter_none_for_bare_client_and_at_depth_exhaustion() -> None:
    limiter = AdaptiveLimiter(initial=2, floor=1, cap=4)
    assert governing_limiter(httpx.AsyncClient()) is None

    wrapped: Any = httpx.AsyncClient(transport=LimitedTransport(limiter=limiter))
    for _ in range(4):
        wrapped = SimpleNamespace(_client=wrapped)
    assert governing_limiter(wrapped) is None  # found only at depth 5
    assert governing_limiter(wrapped, max_depth=5) is limiter


# ── the Limiter protocol ────────────────────────────────────────────────


def test_duck_limiter_satisfies_the_protocol() -> None:
    assert isinstance(RecordingLimiter(), Limiter)
    assert isinstance(AdaptiveLimiter(), Limiter)


def test_partitioned_limiter_lane_works_as_the_limiter() -> None:
    clock = {"now": 0.0}
    shared = PartitionedLimiter(
        AdaptiveLimiter(
            initial=8, floor=1, cap=12, successes_per_increase=1,
            pause_on_throttle=False, clock=lambda: clock["now"],
        ),
        shares={"chat": 1.0},
    )
    lane = shared.lane("chat")
    assert isinstance(lane, Limiter)
    responses = [httpx.Response(200), httpx.Response(429, headers={"retry-after": "1"})]

    def handler(request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    async def main() -> None:
        async with limited_client(lane, handler) as client:
            await client.get("http://test/")  # success feeds the shared loop
            assert shared.limit == 9
            clock["now"] += 61
            await client.get("http://test/")  # 429 halves the shared limit

    asyncio.run(main())
    assert shared.limit == 4
    assert shared.snapshot()["lanes"]["chat"]["active"] == 0


# ── the zero-dependency core ────────────────────────────────────────────


def test_importing_hyperlimit_does_not_import_httpx() -> None:
    code = "import hyperlimit, sys; assert 'httpx' not in sys.modules"
    subprocess.run([sys.executable, "-c", code], check=True)
