# Alternatives and related asyncio libraries

Analysis of other Python asyncio libraries in the adaptive concurrency / rate-limiting space, relative to hyperlimit.

Hyperlimit’s distinctive combination remains relatively rare: AIMD growth/halving driven by explicit throttle signals (especially 429 + Retry-After), pause/cooldown on throttle, per-HTTP-attempt admission (so SDK retries are counted correctly), TokenBudget for TPM, RequestPacer for RPS, PartitionedLimiter for shared quotas, and optional httpx transport integration. Zero dependencies for the core.

This note is for evaluating whether any of these can replace hyperlimit, or whether we can learn useful ideas from them.

## Adaptive / AIMD-style libraries

| Library | Algorithm | Maturity | Key traits | Notes vs hyperlimit |
|---------|-----------|----------|------------|---------------------|
| **aioadaptive** | Vegas (latency-based, Netflix-style) | Very early (alpha) | Context-manager wrapper around an async semaphore; adjusts from RTT/latency. Works with httpx/aiohttp. | Closest conceptual cousin, but delay-based rather than loss/429-based, and immature. |
| **aioresilience** | AIMD on success rate | Early | Configurable increase rate, decrease factor, measurement window, success/failure thresholds. Part of a larger resilience suite (circuit breaker, bulkhead, etc.). | True AIMD, but reacts to success rate rather than explicit throttle signals + Retry-After. |
| **adaptio** | TCP-inspired adaptive concurrency | Early–mid | Dynamically adjusts worker concurrency based on overload signals; has retry decorator. | Similar spirit; less focused on HTTP provider feedback. |
| **Inspect AI `AdaptiveConcurrency`** | AIMD (loss-based, rate-limit aware) | Production (used in eval framework) | Starts at a value, grows while clean, cuts on 429/rate-limit retries, respects Retry-After, cooldown, min/max bounds. | Very close in behavior to hyperlimit’s AIMD; not a standalone reusable library. |
| **adaptive-rate-limiter** | Learns from response headers | Newer | Provider-agnostic (OpenAI-compatible), learns limits from 429 headers, token accounting with refunds, streaming support, optional Redis. | More “discover the published limit” than pure AIMD probing. |

## Fixed-rate / token-bucket / leaky-bucket libraries (non-adaptive)

| Library | Style | Notes |
|---------|-------|-------|
| **aiolimiter** | Leaky bucket | Mature, widely used, simple `async with` API. Excellent for known fixed rates. |
| **asynciolimiter** | Several flavors (Limiter, LeakyBucket, Strict) | Lightweight, accurate rate control. |
| **throttler** | Rate + concurrency context managers | Zero-dep, decorator + context-manager support. |
| **limiter** | Token bucket | Thread-safe + async, jitter. |
| **arate-limit** | Multiple algorithms + optional Redis | More features, distributed option. |
| **throttled-py** | Fixed/sliding window, token/leaky/GCRA + Redis | High-performance, multiple backends. |
| **httpx-limiter** | Transport wrapper around aiolimiter or pyrate-limiter | Integrates rate limits at the httpx transport layer (similar idea to hyperlimit’s LimitedTransport, but fixed rate). |

## Summary

- The closest pure-Python alternatives for the adaptive part are **aioadaptive** (Vegas/latency), **aioresilience** (AIMD on success rate), and the **Inspect AI** adaptive controller (AIMD on rate-limit feedback). None fully replicate the full feature set (especially the httpx-per-attempt + TokenBudget + PartitionedLimiter combo).
- For fixed known rates, **aiolimiter** (or httpx-limiter wrapping it) is the mature default.
- You can mix: use one of the adaptive libraries (or keep hyperlimit) for the provider-facing admission control, and pair it with a durable workflow engine for the outer orchestration.

Sources and further reading can be found by searching the library names on PyPI / GitHub. This analysis was generated as a starting point for deeper comparison.
