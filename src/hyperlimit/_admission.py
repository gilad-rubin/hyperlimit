"""What the httpx and httpx2 transports share: the permit, the verdicts, the lookup.

Neither HTTP package is imported here, so `hyperlimit.httpx` and
`hyperlimit.httpx2` each pull in only their own.
"""

from __future__ import annotations

import sys
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, Protocol, cast, runtime_checkable

_HOLDS = ("headers", "body")


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


@runtime_checkable
class BoundedLimiter(Limiter, Protocol):
    """A `Limiter` whose one wait can be bounded: what `max_wait=` needs.

    `AdaptiveLimiter` and a `PartitionedLimiter` lane both satisfy this.
    """

    async def acquire(self, *, max_wait: float | None = None) -> None: ...

    async def release(self) -> None: ...


class Admission:
    """The transport logic that does not depend on the HTTP package.

    A subclass is also the package's `AsyncBaseTransport`, sets `_default_transport`
    and builds its response-stream wrapper with `release_on_close`.
    """

    limiter: Limiter

    def _setup(self, limiter: Limiter, hold: str, max_wait: float | None) -> None:
        if hold not in _HOLDS:
            raise ValueError(f"hold must be one of {_HOLDS}, got {hold!r}")
        if max_wait is not None:
            if max_wait <= 0:
                raise ValueError(f"max_wait must be positive or None, got {max_wait}")
            if not isinstance(limiter, BoundedLimiter):
                raise TypeError(
                    "max_wait needs a limiter with acquire(max_wait=) and release(), "
                    f"got {type(limiter).__name__}"
                )
        self.limiter = limiter
        self._hold = hold
        self._max_wait = max_wait

    async def _take_permit(self) -> None:
        if self._max_wait is None:
            await self.limiter.__aenter__()
        else:
            await cast(BoundedLimiter, self.limiter).acquire(max_wait=self._max_wait)

    async def _return_permit(self, *exc_info: Any) -> None:
        if self._max_wait is None:
            await self.limiter.__aexit__(*exc_info)
        else:
            await cast(BoundedLimiter, self.limiter).release()

    async def _admit(self, send: Callable[[], Awaitable[Any]], wrap_stream: Callable[[Any], None]) -> Any:
        """One attempt: take a permit, send, record the verdict, hand the permit back."""
        await self._take_permit()
        try:
            response = await send()
            if response.status_code == 429:
                await self.limiter.record_throttle(retry_after=retry_after_from(response.headers))
            elif response.status_code < 400:
                await self.limiter.record_success()
            # Anything else — a rejected schema, a 5xx — teaches the limiter
            # nothing: a bad request is not evidence about capacity. A
            # transport-level failure raises out of here for the same reason.
        except BaseException:
            await self._return_permit(*sys.exc_info())
            raise
        if self._hold == "headers" or response.is_closed:
            # Headers are in, or the body is already in memory: nothing left to hold.
            await self._return_permit(None, None, None)
            return response
        wrap_stream(response)
        return response


def release_on_close(base: type) -> type:
    """A response body class, on the package's `AsyncByteStream`, that hands back its permit once."""

    class PermitReleasingStream(base):  # type: ignore[valid-type, misc]
        """A response body that hands its attempt's permit back when closed, once."""

        def __init__(self, stream: Any, release: Callable[..., Awaitable[None]]) -> None:
            self._stream = stream
            self._release: Callable[..., Awaitable[None]] | None = release

        async def __aiter__(self) -> AsyncIterator[bytes]:
            async for chunk in self._stream:
                yield chunk

        async def aclose(self) -> None:
            try:
                await self._stream.aclose()
            finally:
                release, self._release = self._release, None
                if release is not None:
                    await release(None, None, None)

    return PermitReleasingStream


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
        if isinstance(transport, Admission):
            return transport.limiter
        nested = getattr(seen, "_client", None)
        if nested is None or nested is seen:
            return None
        seen = nested
    return None
