# Changelog

## Unreleased

- **Changed — growth needs load.** A success counts toward the next
  increase only while demand (permits held plus callers waiting) fills at
  least `growth_threshold` of the limit (default `0.5`, Netflix `AIMDLimit`'s
  guard). Before, a lane succeeding one call at a time crept to `cap` and
  fired its next burst into a 429 storm. `growth_threshold=0` restores the
  old counting.
- Bounded waits: `max_wait` on `AdaptiveLimiter` (and per call on
  `acquire` / `Lane.acquire`) raises `AdmissionTimeout` — a `TimeoutError`
  with `waited`, `queued`, `limit`, `limiter`, `lane` — instead of waiting
  forever. On a lane one deadline covers the reserve and the shared permit.
- Telemetry: `observe_limits(fn)` scopes a callback to the current task
  (hypercache's `observe_cache` shape) and receives `Admitted`, `TimedOut`,
  `Released`, `LimitChanged` and `Throttled` events. `name=` labels a
  limiter; `waiting` joins `limit` / `active` and the snapshots.
- `LimitedTransport(hold="body")` keeps the permit until the response body
  closes, so streamed responses are counted. `"headers"` stays the default.
- `LimitedTransport(max_wait=)` bounds one client's wait per HTTP attempt,
  overriding the limiter's default, so clients sharing one lane (live chat,
  background work) wait differently without splitting the quota. Needs a
  `BoundedLimiter` (`acquire(max_wait=)` + `release()`); anything else is
  refused at construction.
- README: 429s that are not about capacity (spend caps without
  `retry-after`).

## 0.3.0 — 2026-08-10

- `hyperlimit.httpx`: HTTP admission module — `LimitedTransport` takes one
  permit per HTTP attempt and reads verdicts off the wire (429 records a
  throttle with the parsed `Retry-After`, `<400` records a success, other
  statuses and transport failures teach nothing); `retry_after_from` parses
  `retry-after-ms` / `retry-after` headers; `governing_limiter` answers
  "which limiter admits this client?" through a wrapper chain. Shipped as an
  optional extra (`hyperlimit[httpx]`); the core stays zero-dependency and
  `import hyperlimit` never imports httpx.
- Versions the previously unreleased `RequestPacer` (requests-per-second
  admission via a capacity-one token bucket), which landed after 0.2.0
  without a release.

## 0.2.0

- Throttle response: cooldown window absorbs a burst into one cut, pauses
  new admissions, honors sane server `retry_after`; `jitter` de-synchronizes
  parallel resumers.
- Growth modes: `growth="fixed"` (default) and `growth="sqrt"`.
- `TokenBudget`: continuous-refill token bucket for per-minute quotas.
- `PartitionedLimiter`: named lanes with reserved shares of one adaptive
  limit, one AIMD loop, one pause.

## 0.1.0

- `AdaptiveLimiter`: adaptive (AIMD) asyncio concurrency limiter — additive
  increase on recorded successes, multiplicative decrease on recorded
  throttles, injectable clock/sleep/rng.
