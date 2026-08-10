# hyperlimit

Adaptive (AIMD) concurrency limiter for asyncio — TCP congestion control for
your provider calls. Instead of configuring the "right" concurrency constant,
the limiter discovers it: **additive increase** (+1 permit after N clean
completions) while work succeeds, **multiplicative decrease** (halve, with a
floor and a cool-down) when the workload reports a throttle signal (429s,
connection storms).

Born from a production incident: a fixed per-batch limit of 3 multiplied into
~30 concurrent jobs across parallel batches, drew a rate-limit storm,
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

## RequestPacer

When a provider quota is requests per second rather than concurrent work,
`RequestPacer` admits at a steady rate without holding a permit for the
operation's lifetime:

```python
from hyperlimit import RequestPacer

pacer = RequestPacer(requests_per_second=15)

async def submit(request):
    await pacer.acquire()
    return await provider.submit(request)
```

Its capacity-one token bucket allows one request immediately after idle time,
then spaces concurrent callers evenly. Injectable `clock` and `sleep` keep
rate contracts deterministic in tests.

## HTTP admission (httpx)

For HTTP workloads the right place to hold a permit is the transport: SDK
clients retry internally, so gating the outer call holds one permit across
every attempt and backoff sleep — and the intermediate 429s, the ones that
carry `Retry-After`, never reach the limiter. `hyperlimit.httpx` (optional
extra: `pip install "hyperlimit[httpx]"`; the core stays zero-dependency)
admits one permit per HTTP attempt and reads verdicts off the wire:

```python
import httpx
from hyperlimit import AdaptiveLimiter
from hyperlimit.httpx import LimitedTransport

limiter = AdaptiveLimiter(initial=3, floor=1, cap=12)
client = httpx.AsyncClient(transport=LimitedTransport(limiter=limiter))

response = await client.post("https://api.example.com/v1/things", json=payload)
```

Every 429 records a throttle with the parsed `Retry-After` (`retry-after-ms`
wins over `retry-after`; HTTP-dates are ignored), every `<400` response
records a success, and anything else — a rejected request, a 5xx, a
transport failure — teaches the limiter nothing: a bad request is not
evidence about capacity. The permit covers connect, send, and response
headers; body reads and streaming are not gated. Any object with the permit
and verdict methods works as the `limiter` — an `AdaptiveLimiter`, a
`PartitionedLimiter` lane, or your own wrapper (`hyperlimit.httpx.Limiter`
is the protocol).

`governing_limiter(client)` answers "which limiter admits this client's
attempts?" by best-effort introspection through wrapper chains (caching
proxy → SDK client → httpx client → transport), so "was the limiter
actually wired?" stays checkable.

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
