"""LimitedTransport on httpx2: the same lane semantics as the httpx one, and the anthropic 1.x client over it."""

from __future__ import annotations

import asyncio
import subprocess
import sys
from types import SimpleNamespace

import httpx2
import pytest

from hyperlimit import AdaptiveLimiter
from hyperlimit.httpx2 import LimitedTransport, governing_limiter


def test_permit_covers_one_attempt_and_verdicts_come_from_the_wire() -> None:
    lane = AdaptiveLimiter(initial=2, floor=1, cap=4)
    statuses = iter([429, 200])
    seen: list[int] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        seen.append(lane.active)
        status = next(statuses)
        return httpx2.Response(status, headers={"retry-after-ms": "10"} if status == 429 else {})

    async def main() -> None:
        async with httpx2.AsyncClient(
            transport=LimitedTransport(limiter=lane, transport=httpx2.MockTransport(handler))
        ) as client:
            assert (await client.get("https://x.test")).status_code == 429
            assert (await client.get("https://x.test")).status_code == 200

    asyncio.run(main())
    assert seen == [1, 1]
    assert lane.active == 0


class GatedStream(httpx2.AsyncByteStream):
    """A streamed body whose chunks arrive only when the test opens the gate."""

    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.gate = asyncio.Event()
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            await self.gate.wait()
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


def test_hold_body_keeps_the_permit_until_the_body_closes() -> None:
    lane = AdaptiveLimiter(initial=2, floor=1, cap=4)
    body = GatedStream([b"a", b"b"])

    async def main() -> None:
        transport = LimitedTransport(
            limiter=lane, transport=httpx2.MockTransport(lambda r: httpx2.Response(200, stream=body)), hold="body"
        )
        async with httpx2.AsyncClient(transport=transport) as client:
            async with client.stream("GET", "https://x.test") as response:
                assert lane.active == 1  # headers are in, the body is not
                body.gate.set()
                assert await response.aread() == b"ab"
                assert lane.active == 0
            assert body.closed

    asyncio.run(main())


def test_transport_raise_releases_the_permit() -> None:
    lane = AdaptiveLimiter(initial=2, floor=1, cap=4)

    def handler(request: httpx2.Request) -> httpx2.Response:
        raise httpx2.ConnectError("down")

    async def main() -> None:
        client = httpx2.AsyncClient(transport=LimitedTransport(limiter=lane, transport=httpx2.MockTransport(handler)))
        with pytest.raises(httpx2.ConnectError):
            await client.get("https://x.test")

    asyncio.run(main())
    assert lane.active == 0


def test_governing_limiter_finds_the_lane_and_validates_options() -> None:
    lane = AdaptiveLimiter(initial=2, floor=1, cap=4)
    client = httpx2.AsyncClient(transport=LimitedTransport(limiter=lane))
    assert governing_limiter(client) is lane
    assert governing_limiter(SimpleNamespace(_client=client)) is lane
    assert governing_limiter(httpx2.AsyncClient()) is None
    with pytest.raises(ValueError):
        LimitedTransport(limiter=lane, hold="never")
    with pytest.raises(ValueError):
        LimitedTransport(limiter=lane, max_wait=0)


def test_anthropic_async_client_makes_a_mocked_call_through_the_lane() -> None:
    anthropic = pytest.importorskip("anthropic")
    lane = AdaptiveLimiter(initial=2, floor=1, cap=4)
    attempts: list[int] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        attempts.append(1)
        if len(attempts) == 1:
            return httpx2.Response(429, headers={"retry-after-ms": "10"}, json={
                "type": "error", "error": {"type": "rate_limit_error", "message": "slow down"}})
        return httpx2.Response(200, json={
            "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-haiku-5-5",
            "content": [{"type": "text", "text": "ok"}], "stop_reason": "end_turn", "stop_sequence": None,
            "usage": {"input_tokens": 1, "output_tokens": 1}})

    async def main() -> str:
        client = anthropic.AsyncAnthropic(
            api_key="sk-test", max_retries=2,
            http_client=httpx2.AsyncClient(
                transport=LimitedTransport(limiter=lane, transport=httpx2.MockTransport(handler))),
        )
        assert governing_limiter(client) is lane
        reply = await client.messages.create(
            model="claude-haiku-5-5", max_tokens=8, messages=[{"role": "user", "content": "hi"}])
        return reply.content[0].text

    assert asyncio.run(main()) == "ok"
    assert len(attempts) == 2  # the SDK retried; both attempts were admitted
    assert lane.active == 0


def test_importing_the_httpx_transport_does_not_import_httpx2() -> None:
    code = "import hyperlimit.httpx, sys; assert 'httpx2' not in sys.modules"
    subprocess.run([sys.executable, "-c", code], check=True)


def test_importing_the_httpx2_transport_does_not_import_httpx() -> None:
    code = "import hyperlimit.httpx2, sys; assert 'httpx' not in sys.modules"
    subprocess.run([sys.executable, "-c", code], check=True)
