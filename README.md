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
        await limiter.record_throttle(retry_after=outcome.retry_after)
    else:
        await limiter.record_success()    # +1 after N of these (up to cap)
```

- Zero dependencies, asyncio-native.
- The permit (`async with`) and the verdict (`record_success` /
  `record_throttle`) are deliberately separate: the caller decides what
  counts as a throttle signal.
- `on_change(old, new, reason)` hook for logging limit moves.
- Injectable `clock`, `sleep`, and `rng` for deterministic tests.

## Throttle response

A throttle opens a **cooldown window** that does three things at once:

- absorbs further throttles from the same burst into one cut (one storm,
  one halving — not a freefall);
- **pauses new admissions** until it ends — queued waiters stop firing into
  the storm; permits already held keep running (`pause_on_throttle=False`
  restores halve-only behavior);
- lasts exactly as long as the server asked when the caller passes a sane
  `retry_after` (`0 < value <= max_retry_after`, default 60 s); insane
  values fall back to `cooldown_seconds`.

`jitter=0.1` stretches each window by up to 10 % (via the injectable `rng`)
so parallel lanes or processes don't resume in lockstep.

## Growth modes

`growth="fixed"` (default) adds one permit per increase. `growth="sqrt"`
adds `max(1, int(sqrt(limit)))` — ramps a wide lane fast while staying
gentle near small limits (Envoy's headroom heuristic).

## TokenBudget

LLM providers throttle on tokens-per-minute more than on concurrency, so a
permit count alone admits bursts the quota cannot absorb. `TokenBudget` is a
continuous-refill bucket to hold beside the permits:

```python
from hyperlimit import TokenBudget

budget = TokenBudget(tokens_per_minute=90_000)

async def call(request):
    estimate = estimate_tokens(request)
    await budget.spend(estimate)          # waits for refill when dry
    response = await provider(request)
    budget.adjust(estimate - response.usage.total_tokens)  # reconcile
```

A request bigger than a full bucket is admitted when the bucket is full and
leaves a debt, which honestly delays followers instead of deadlocking.

## PartitionedLimiter

Lanes that share one provider quota must not run independent AIMD loops —
one lane's throttle teaches the other nothing, so it keeps pushing into the
same storm and both oscillate. `PartitionedLimiter` keeps ONE limiter (one
limit, one loop, one pause) and gives each lane a reserved share of it,
recomputed from the live limit at every admission:

```python
from hyperlimit import AdaptiveLimiter, PartitionedLimiter

shared = PartitionedLimiter(
    AdaptiveLimiter(initial=10, floor=2, cap=20),
    shares={"chat": 0.8, "embeddings": 0.2},
)
chat = shared.lane("chat")

async with chat:                      # bounded by the chat reserve AND the total
    response = await provider(request)
await chat.record_success()           # verdicts feed the one shared loop
```

Reserves are strict, not work-conserving; size the parent floor at or above
the lane count so every lane keeps a live reserve.
