"""HTTP admission for httpx clients: one transport, its verdicts.

Admission lives in the client's transport, one permit per HTTP attempt.
Wrapping a component method instead would hold one permit across every
internal retry an SDK client performs — and the intermediate 429s, the ones
that carry `Retry-After`, would never reach the limiter at all. Here every
attempt is its own admission and every 429 is a verdict.

This module is an optional extra: `hyperlimit` itself never imports it, so
the core package stays dependency-free. Install with `hyperlimit[httpx]`.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

import httpx


@runtime_checkable
class Limiter(Protocol):
    """What the transport needs from a limiter: a permit and two verdicts.

    `AdaptiveLimiter` and a `PartitionedLimiter` lane both satisfy this, and
    so does any recording or forwarding wrapper that keeps the four methods.
    """

    async def __aenter__(self) -> Any: ...

    async def __aexit__(self, *exc_info: Any) -> Any: ...

    async def record_success(self) -> None: ...

    async def record_throttle(self, *, retry_after: float | None = None) -> None: ...


class LimitedTransport(httpx.AsyncBaseTransport):
    """One limiter, wrapped around ONE HTTP attempt.

    Admission belongs here rather than around a higher-level call because
    SDK clients retry INSIDE that call: one permit would be held across
    every attempt and every backoff sleep between them — a request that had
    stopped talking to the server still shrinking everyone else's share of
    the quota — and only the final failure would ever reach the limiter.
    Here every attempt is its own admission and every 429 is a verdict,
    which also puts the limiter's cooldown in the retry path: the next
    attempt waits for admission instead of firing into the storm.

    The permit covers connect, send, and waiting for the server's response
    headers. Reading the body afterwards is not gated, and neither is a
    streamed response.
    """

    def __init__(
        self,
        *,
        limiter: Limiter,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.limiter = limiter
        self._transport = transport or httpx.AsyncHTTPTransport()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        async with self.limiter:
            response = await self._transport.handle_async_request(request)
            if response.status_code == 429:
                await self.limiter.record_throttle(
                    retry_after=retry_after_from(response.headers)
                )
            elif response.status_code < 400:
                await self.limiter.record_success()
            # Anything else — a rejected schema, a 5xx — teaches the limiter
            # nothing: a bad request is not evidence about capacity. A
            # transport-level failure raises out of here for the same reason.
            return response

    async def aclose(self) -> None:
        await self._transport.aclose()


def retry_after_from(headers: Any) -> float | None:
    """How long the server asked us to wait, if it said.

    `retry-after-ms` (milliseconds) wins over `retry-after` (seconds).
    HTTP-date forms and garbage yield None.
    """
    if headers is None:
        return None
    try:
        if headers.get("retry-after-ms"):
            return float(headers["retry-after-ms"]) / 1000.0
        if headers.get("retry-after"):
            return float(headers["retry-after"])  # HTTP-date forms fall through
    except (TypeError, ValueError):
        return None
    return None


def governing_limiter(client: Any, *, max_depth: int = 4) -> Limiter | None:
    """Which limiter admits this client's attempts, or None if it is ungated.

    Best-effort introspection: neither httpx nor the SDK clients built on it
    expose the transport they ride, and "was the limiter actually wired?"
    has to stay answerable — a limiter argument nothing consumed is exactly
    the bug this check exists to catch. The coupling to those internals
    lives HERE, in the module that installed the transport, rather than at
    every caller that wants to check.

    The chain is walked because SDK clients are often handed WRAPPED: a
    caching proxy over an SDK client over an httpx client over the
    transport. `max_depth` bounds the walk before it gives up.
    """
    seen = client
    for _ in range(max_depth):
        transport = getattr(seen, "_transport", None)
        if isinstance(transport, LimitedTransport):
            return transport.limiter
        nested = getattr(seen, "_client", None)
        if nested is None or nested is seen:
            return None
        seen = nested
    return None
