"""Scoped admission telemetry: who waited, how long, and why the limit moved.

`observe_limits(fn)` installs a callback for the current thread or async task
(a ``ContextVar``, the same shape as hypercache's ``observe_cache``), so a
request or workflow run can collect the admissions it caused and attach them
to its own trace. Events are emitted in the task that caused them: an
admission in the task that waited, a limit move in the task that recorded the
verdict. Nothing is built when no observer is installed.

An observer that raises is logged and ignored: telemetry never changes what
the limiter admits.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Admitted:
    """A caller got a permit.

    Attributes:
        limiter: The limiter's `name=`, if it was given one.
        lane: The `PartitionedLimiter` lane admitted through, else None.
        waited: Seconds between asking and holding the permit.
        queued: Callers already waiting when this one arrived.
        limit: The limit at admission.
        active: Permits held, this one included.
    """

    limiter: str | None
    lane: str | None
    waited: float
    queued: int
    limit: int
    active: int


@dataclass(frozen=True)
class TimedOut:
    """A caller gave up at its `max_wait`; `AdmissionTimeout` was raised.

    Attributes mirror `Admitted`; `active` is the permits held by others.
    """

    limiter: str | None
    lane: str | None
    waited: float
    queued: int
    limit: int
    active: int


@dataclass(frozen=True)
class Released:
    """A permit was handed back.

    Attributes:
        limiter: The limiter's `name=`, if it was given one.
        lane: The `PartitionedLimiter` lane the permit belonged to, else None.
        limit: The limit at release.
        active: Permits still held.
        waiting: Callers still waiting.
    """

    limiter: str | None
    lane: str | None
    limit: int
    active: int
    waiting: int


@dataclass(frozen=True)
class LimitChanged:
    """The adaptive limit moved.

    Attributes:
        limiter: The limiter's `name=`, if it was given one.
        old: The limit before.
        new: The limit after.
        reason: "additive-increase" or "multiplicative-decrease".
    """

    limiter: str | None
    old: int
    new: int
    reason: str


@dataclass(frozen=True)
class Throttled:
    """A throttle verdict was recorded.

    Attributes:
        limiter: The limiter's `name=`, if it was given one.
        retry_after: What the caller passed (the server's ask), if anything.
        window: Seconds of cooldown this throttle opened; None when it fell
            inside an open window and was absorbed into the earlier cut.
        limit: The limit after the verdict.
    """

    limiter: str | None
    retry_after: float | None
    window: float | None
    limit: int


LimitEvent = Admitted | TimedOut | Released | LimitChanged | Throttled
LimitObserver = Callable[[LimitEvent], None]

_observer: ContextVar[LimitObserver | None] = ContextVar("hyperlimit_observer", default=None)


@contextmanager
def observe_limits(fn: LimitObserver) -> Iterator[None]:
    """Observe admission telemetry within the current context.

    Args:
        fn: Callback invoked with each `LimitEvent` emitted inside the scope.
    """
    token = _observer.set(fn)
    try:
        yield
    finally:
        _observer.reset(token)


def _active_observer() -> LimitObserver | None:
    return _observer.get()


def _deliver(observer: LimitObserver, event: LimitEvent) -> None:
    try:
        observer(event)
    except Exception:
        log.warning("hyperlimit observer raised; ignoring", exc_info=True)
