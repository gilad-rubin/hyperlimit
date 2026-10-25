"""HTTP admission for httpx clients: one transport, its verdicts.

Admission lives in the client's transport, one permit per HTTP attempt.
Wrapping a component method instead would hold one permit across every
internal retry an SDK client performs — and the intermediate 429s, the ones
that carry `Retry-After`, would never reach the limiter at all. Here every
attempt is its own admission and every 429 is a verdict.

This module is an optional extra: `hyperlimit` itself never imports it, so
the core package stays dependency-free. Install with `hyperlimit[httpx]`. An SDK built on `httpx2` (anthropic 1.x)
takes `hyperlimit.httpx2.LimitedTransport` instead: the same class on the other package.
"""

from __future__ import annotations

from typing import Any

import httpx

from hyperlimit._admission import (
    Admission,
    BoundedLimiter,
    Limiter,
    governing_limiter,
    release_on_close,
    retry_after_from,
)

__all__ = ["BoundedLimiter", "LimitedTransport", "Limiter", "governing_limiter", "retry_after_from"]

_PermitReleasingStream = release_on_close(httpx.AsyncByteStream)


class LimitedTransport(Admission, httpx.AsyncBaseTransport):
    """One limiter, wrapped around ONE HTTP attempt.

    Admission belongs here rather than around a higher-level call because
    SDK clients retry INSIDE that call: one permit would be held across
    every attempt and every backoff sleep between them — a request that had
    stopped talking to the server still shrinking everyone else's share of
    the quota — and only the final failure would ever reach the limiter.
    Here every attempt is its own admission and every 429 is a verdict,
    which also puts the limiter's cooldown in the retry path: the next
    attempt waits for admission instead of firing into the storm.

    `hold` says how long one attempt occupies its permit:

    - `"headers"` (default): connect, send, and waiting for the server's
      response headers. Reading the body afterwards is not gated.
    - `"body"`: until the response body is closed — read to the end, closed
      early, or abandoned through an error or cancellation. Use it for
      streamed responses (a chat answer streaming for many seconds), which
      otherwise run uncounted. httpx closes the body for you on a plain
      request and when a `client.stream(...)` block exits; a stream that is
      never closed keeps its permit.

    Verdicts come from the status code either way, as soon as headers arrive.

    `max_wait` bounds THIS client's wait for a permit, per HTTP attempt,
    overriding the limiter's own default; past it `AdmissionTimeout` raises
    out of the request. Clients sharing one limiter can wait differently —
    a live chat client gives up while a background client waits its turn —
    without splitting the quota. It needs a `BoundedLimiter`.
    """

    def __init__(
        self,
        *,
        limiter: Limiter,
        transport: httpx.AsyncBaseTransport | None = None,
        hold: str = "headers",
        max_wait: float | None = None,
    ) -> None:
        self._setup(limiter, hold, max_wait)
        self._transport = transport or httpx.AsyncHTTPTransport()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        def wrap(response: httpx.Response) -> None:
            response.stream = _PermitReleasingStream(response.stream, self._return_permit)

        return await self._admit(lambda: self._transport.handle_async_request(request), wrap)

    async def aclose(self) -> None:
        await self._transport.aclose()
