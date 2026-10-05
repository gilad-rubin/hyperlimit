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

from hyperlimit import AdaptiveLimiter, AdmissionTimeout, PartitionedLimiter
from hyperlimit.httpx import (
    BoundedLimiter,
    Limiter,
    LimitedTransport,
    governing_limiter,
    retry_after_from,
)

from tests._fake_time import FakeTime, drain


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
            initial=8, floor=1, cap=12, successes_per_increase=1, growth_threshold=0.0,
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


# ── hold="body": the permit lasts until the body closes ────────────────


class GatedStream(httpx.AsyncByteStream):
    """A streamed body whose chunks arrive only when the test opens the gate."""

    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.gate = asyncio.Event()
        self.closed = False

    async def __aiter__(self) -> Any:
        for chunk in self.chunks:
            await self.gate.wait()
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


def body_client(limiter: Limiter, handler: Callable[[httpx.Request], httpx.Response]) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=LimitedTransport(
            limiter=limiter, transport=httpx.MockTransport(handler), hold="body"
        )
    )


def test_hold_body_keeps_the_permit_until_the_stream_is_read() -> None:
    limiter = AdaptiveLimiter(initial=3, floor=1, cap=8)
    body = GatedStream([b"a", b"b"])

    async def main() -> None:
        async with body_client(limiter, lambda request: httpx.Response(200, stream=body)) as client:
            async with client.stream("GET", "http://test/") as response:
                assert limiter.active == 1  # headers are in, the body is not
                body.gate.set()
                assert await response.aread() == b"ab"
                assert limiter.active == 0  # read to the end: handed back
            assert body.closed

    asyncio.run(main())


def test_hold_body_releases_when_the_stream_is_closed_early() -> None:
    limiter = AdaptiveLimiter(initial=3, floor=1, cap=8)
    body = GatedStream([b"a", b"b"])

    async def main() -> None:
        async with body_client(limiter, lambda request: httpx.Response(200, stream=body)) as client:
            async with client.stream("GET", "http://test/"):
                assert limiter.active == 1
            assert limiter.active == 0  # never read: the block's exit closes it

    asyncio.run(main())


def test_hold_body_releases_when_the_reader_is_cancelled() -> None:
    limiter = AdaptiveLimiter(initial=3, floor=1, cap=8)
    body = GatedStream([b"a", b"b"])

    async def main() -> None:
        async with body_client(limiter, lambda request: httpx.Response(200, stream=body)) as client:

            async def reader() -> None:
                async with client.stream("GET", "http://test/") as response:
                    async for _ in response.aiter_bytes():
                        pass

            task = asyncio.create_task(reader())
            for _ in range(10):
                await asyncio.sleep(0)
            assert limiter.active == 1  # parked mid-stream
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert limiter.active == 0

    asyncio.run(main())


def test_hold_body_releases_once_even_if_closed_twice() -> None:
    limiter = RecordingLimiter()
    body = GatedStream([b"a"])

    async def main() -> None:
        transport = LimitedTransport(
            limiter=limiter,
            transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=body)),
            hold="body",
        )
        response = await transport.handle_async_request(httpx.Request("GET", "http://test/"))
        assert limiter.active == 1
        await response.stream.aclose()
        await response.stream.aclose()
        assert limiter.active == 0

    asyncio.run(main())


def test_hold_body_releases_at_once_when_the_body_is_already_in_memory() -> None:
    limiter = AdaptiveLimiter(initial=3, floor=1, cap=8)

    async def main() -> None:
        transport = LimitedTransport(
            limiter=limiter,
            transport=httpx.MockTransport(lambda request: httpx.Response(200, content=b"x")),
            hold="body",
        )
        await transport.handle_async_request(httpx.Request("GET", "http://test/"))
        assert limiter.active == 0

    asyncio.run(main())


def test_hold_body_still_takes_the_verdict_at_the_headers() -> None:
    limiter = RecordingLimiter()
    body = GatedStream([b"slow down"])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"retry-after": "2"}, stream=body)

    async def main() -> None:
        async with body_client(limiter, handler) as client:
            async with client.stream("GET", "http://test/"):
                assert limiter.throttles == [2.0]  # before any body byte

    asyncio.run(main())
    assert limiter.active == 0


def test_hold_body_releases_when_the_attempt_raises() -> None:
    limiter = RecordingLimiter()

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("wire down")

    async def main() -> None:
        async with body_client(limiter, handler) as client:
            with pytest.raises(httpx.ConnectError):
                await client.get("http://test/")

    asyncio.run(main())
    assert limiter.active == 0


def test_unknown_hold_is_rejected() -> None:
    with pytest.raises(ValueError):
        LimitedTransport(limiter=RecordingLimiter(), hold="forever")


# ── max_wait: one client's bound on a shared limiter ───────────────────


def test_max_wait_bounds_one_client_while_another_on_the_same_limiter_waits() -> None:
    fake = FakeTime()
    limiter = AdaptiveLimiter(initial=1, floor=1, cap=4, clock=fake.clock, sleep=fake.sleep)
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return httpx.Response(200)

    chat = httpx.AsyncClient(
        transport=LimitedTransport(
            limiter=limiter, transport=httpx.MockTransport(handler), max_wait=2.0
        )
    )
    batch = limited_client(limiter, handler)

    async def main() -> None:
        await limiter.acquire()  # the one permit is busy
        live = asyncio.create_task(chat.get("http://test/chat"))
        background = asyncio.create_task(batch.get("http://test/batch"))
        await drain()
        fake.advance(2.0)
        await drain()
        assert live.done()  # the live client gave up at its bound
        with pytest.raises(AdmissionTimeout):
            await live
        assert not background.done()  # the background client waits its turn
        await limiter.release()
        await drain()
        assert (await background).status_code == 200
        await chat.aclose()
        await batch.aclose()

    asyncio.run(main())
    assert calls == ["/batch"]  # the timed-out attempt never reached the wire
    assert (limiter.active, limiter.waiting) == (0, 0)


def test_transport_max_wait_overrides_the_limiter_default() -> None:
    fake = FakeTime()
    limiter = AdaptiveLimiter(
        initial=1, floor=1, cap=4, max_wait=100.0, clock=fake.clock, sleep=fake.sleep
    )
    transport = LimitedTransport(
        limiter=limiter,
        transport=httpx.MockTransport(lambda request: httpx.Response(200)),
        max_wait=1.0,
    )

    async def main() -> None:
        await limiter.acquire()
        attempt = asyncio.create_task(
            transport.handle_async_request(httpx.Request("GET", "http://test/"))
        )
        await drain()
        fake.advance(1.0)
        await drain()
        assert attempt.done()
        with pytest.raises(AdmissionTimeout) as caught:
            await attempt
        assert caught.value.waited == 1.0

    asyncio.run(main())


def test_max_wait_and_hold_body_hand_the_permit_back_when_the_body_closes() -> None:
    limiter = AdaptiveLimiter(initial=3, floor=1, cap=8)
    body = GatedStream([b"a"])
    client = httpx.AsyncClient(
        transport=LimitedTransport(
            limiter=limiter,
            transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=body)),
            hold="body",
            max_wait=5.0,
        )
    )

    async def main() -> None:
        async with client, client.stream("GET", "http://test/") as response:
            assert limiter.active == 1
            body.gate.set()
            await response.aread()
            assert limiter.active == 0

    asyncio.run(main())


def test_max_wait_through_a_lane_names_the_lane() -> None:
    fake = FakeTime()
    shared = PartitionedLimiter(
        AdaptiveLimiter(initial=2, floor=1, cap=4, clock=fake.clock, sleep=fake.sleep),
        shares={"chat": 0.5, "batch": 0.5},
    )
    chat = shared.lane("chat")
    transport = LimitedTransport(
        limiter=chat,
        transport=httpx.MockTransport(lambda request: httpx.Response(200)),
        max_wait=3.0,
    )

    async def main() -> None:
        await chat.acquire()  # the chat reserve (1) is full
        attempt = asyncio.create_task(
            transport.handle_async_request(httpx.Request("GET", "http://test/"))
        )
        await drain()
        fake.advance(3.0)
        await drain()
        assert attempt.done()
        with pytest.raises(AdmissionTimeout) as caught:
            await attempt
        assert caught.value.lane == "chat"

    asyncio.run(main())


def test_max_wait_needs_a_bounded_limiter() -> None:
    assert isinstance(AdaptiveLimiter(), BoundedLimiter)
    lane = PartitionedLimiter(AdaptiveLimiter(), shares={"a": 1.0}).lane("a")
    assert isinstance(lane, BoundedLimiter)
    with pytest.raises(TypeError):
        LimitedTransport(limiter=RecordingLimiter(), max_wait=5.0)
    with pytest.raises(ValueError):
        LimitedTransport(limiter=AdaptiveLimiter(), max_wait=0)
    LimitedTransport(limiter=RecordingLimiter())  # unbounded: the duck limiter still fits


# ── the zero-dependency core ────────────────────────────────────────────


def test_importing_hyperlimit_does_not_import_httpx() -> None:
    code = "import hyperlimit, sys; assert 'httpx' not in sys.modules"
    subprocess.run([sys.executable, "-c", code], check=True)
