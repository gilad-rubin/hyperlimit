# Changelog

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
