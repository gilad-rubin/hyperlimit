# hyperlimit

Adaptive (AIMD) concurrency limiter for asyncio — TCP congestion control for
your provider calls. Instead of configuring the "right" concurrency constant,
the limiter discovers it: **additive increase** (+1 permit after N clean
completions) while work succeeds, **multiplicative decrease** (halve, with a
floor and a cool-down) when the workload reports a throttle signal (429s,
connection storms).

Born from a production incident: a fixed per-batch limit of 3 multiplied into
~30 concurrent documents across parallel batches, drew a rate-limit storm,
and the "safe" manual fallback left throughput on the table. The right limit
moves with the provider's load — so probe for it.

```python
from hyperlimit import AdaptiveLimiter

limiter = AdaptiveLimiter(initial=3, floor=2, cap=12, successes_per_increase=8)

async def work(item):
    async with limiter:           # holds one permit
        outcome = await process(item)
    if outcome.throttled:
        await limiter.record_throttle()   # halve (respects floor + cooldown)
    else:
        await limiter.record_success()    # +1 after N of these (up to cap)
```

- Zero dependencies, single module, asyncio-native.
- The permit (`async with`) and the verdict (`record_success` /
  `record_throttle`) are deliberately separate: the caller decides what
  counts as a throttle signal.
- `on_change(old, new, reason)` hook for logging limit moves.
- Injectable monotonic clock for deterministic cooldown tests.
