# Prompt for Claude Opus 4.6 — F5 GTM Automation Service Enhancements

You are enhancing a **production F5 GTM automation service** that already exists and works.
Read all existing code thoroughly before writing a single line.

This service provisions GSLB configuration on **F5 BIG-IP DNS** — WideIP, pool, pool members,
monitor — and **CNAME records in Infoblox**, triggered by REST API calls. Every OpenShift
deployment depends on it.

The existing service has the basic end-to-end flow working: API → Celery task → F5 calls →
Infoblox call → rollback on failure. Your job is to make it production-safe under load,
concurrent requests, partial failures, and device instability.

---

## STEP 1 — Read and understand before touching anything

Read every file in the codebase. Build a mental model of:
- How a request flows from POST to F5 to Infoblox and back
- How rollback currently works
- How Celery tasks are enqueued and what they receive
- How Redis is used today
- How F5 authentication works today

Do not write any code until you have read everything.

---

## STEP 2 — Fix these critical issues first (before any enhancements)

### Critical issue 1 — Redis eviction policy

Run this against every Redis instance the service connects to:
```
CONFIG GET maxmemory-policy
```

If the result is anything other than `noeviction` — **fix this immediately before anything else.**

**Why this will silently destroy your service:** Any `allkeys-*` policy (the common default)
tells Redis to delete keys to make room when memory is tight. Redis does not distinguish
a Celery task payload from a cache entry. Your queued tasks disappear with no error raised
anywhere. Workers sit idle. Requests are lost. No log entry. No exception. Just silence.

Also check `maxmemory`. If it is `0` (unlimited), Redis will grow until the container
OOM-killer terminates the pod — likely the cause of any unexplained pod restarts you've seen.
Set `maxmemory` to 60–70% of the container's memory limit. Redis forks on RDB snapshots
and copy-on-write can transiently double memory. Leave headroom.

Required Redis config:
```
maxmemory              <60-70% of your container memory limit>
maxmemory-policy       noeviction
appendonly             yes
appendfsync            everysec
```

With `noeviction`, writes fail with an OOM error instead of silently succeeding. Your API
must catch this and return `503 + Retry-After`, not a `500`.

### Critical issue 2 — F5 authentication

Check how the existing code authenticates to F5. If it calls the login endpoint on
every request, that is both slow and fragile.

The correct pattern:
1. `POST /mgmt/shared/authn/login` with `{"username":..., "password":..., "loginProviderName":...}`
2. Response contains a token. Immediately extend it to 36000 seconds:
   `PATCH /mgmt/shared/authn/tokens/{token}` with `{"timeout": 36000}`
3. Cache the token in Redis with a TTL slightly under 36000 seconds
4. All subsequent calls use `X-F5-Auth-Token: {token}` header

**The `loginProviderName` field is critical and must be configurable.** If your deployment
uses TACACS+ for admin authentication (likely), the value is NOT `"tmos"` — it is the
name of the TACACS+ auth source object on the BIG-IP. Find it by running
`tmsh list auth tacacs` on the device. Hardcoding `"tmos"` will cause auth failures silently
or produce confusing 401 errors.

Add a config variable `F5_LOGIN_PROVIDER_NAME` (environment variable or config file).
Never hardcode the value.

**Stampede guard:** If 4 pods all try to refresh the token simultaneously, they each make
a login call and overwrite each other's cached token. Prevent this with a Redis NX lock:
```python
# Only one pod refreshes the token; others wait and read the result
acquired = await redis.set(f"token_lock:{device_id}", "1", nx=True, ex=30)
if acquired:
    token = await call_f5_login()
    await redis.set(f"f5_token:{device_id}", token, ex=35000)
    await redis.delete(f"token_lock:{device_id}")
else:
    # Wait for the lock holder to finish, then read the cached token
    await asyncio.sleep(1)
    token = await redis.get(f"f5_token:{device_id}")
```

### Critical issue 3 — What Celery tasks receive

Check what your `.delay()` call passes to the worker. If it passes the full request payload,
change it to pass only the `request_id`. The worker loads the full request from the database.

Why: A queue of 5,000 request IDs is a few megabytes. A queue of 5,000 GTM payloads is
potentially gigabytes — and is likely the primary driver of Redis memory pressure.

```python
# Wrong — passes payload through Redis
run_workflow.delay(request_id=str(id), payload=request.payload)

# Correct — only the ID; worker loads from DB
run_workflow.delay(request_id=str(id), device_id=str(device))
```

Also set `task_ignore_result = True` in Celery config. Status already lives in your database;
storing results in Redis too doubles the memory usage per task.

Also set `worker_prefetch_multiplier = 1`. The default (4) means a worker grabs 4 tasks
at once. At P-1 = 8 concurrent workers, that is 32 tasks claimed but only 8 processing —
the other 24 are invisible to other pods and will never be processed if the worker dies.

---

## STEP 3 — Enhancements to add

### Enhancement A — Make every F5 and Infoblox operation idempotent

Check every write call in the existing code. If any of them write without first reading,
they are not idempotent and will error on retry.

The mandatory pattern for every create/update:
```python
existing = await f5_client.get_wideip(name)   # read current state

if existing is None:
    await f5_client.create_wideip(payload)
    return "created"

elif fields_differ(existing, payload):         # compare
    await f5_client.update_wideip(name, payload)
    return "updated"

else:
    return "no_op"                             # identical — do nothing, succeed silently
```

The mandatory pattern for every delete:
```python
existing = await f5_client.get_wideip(name)
if existing is None:
    return "no_op"   # already gone — succeed silently, do not raise
await f5_client.delete_wideip(name)
return "deleted"
```

**The no-op branch is not optional.** Without it, every retry after a transient failure
creates a duplicate or raises a "already exists" error. With it, retries are safe.

Object ordering matters. F5 requires dependencies to exist before dependent objects:
```
CREATE order:  monitor → pool → pool members → WideIP → CNAME (Infoblox)
DELETE order:  CNAME → WideIP → pool members → pool → monitor
```
CNAME must be removed **before** WideIP on delete. If WideIP is removed first, the CNAME
points at nothing and DNS resolution breaks immediately for all consumers.

**BIG-IP 17.1.x GTM objects do NOT support iControl REST transactions.** Each object is a
separate API call. There is no atomic multi-object transaction available. Design accordingly.

### Enhancement B — Strengthen rollback with pre-state capture

The existing rollback likely knows what to undo but may not know what state to restore to.
The rule is: **never delete an object that existed before the request arrived**.

Before every write, capture the current state:
```python
# Capture BEFORE any write
existing = await f5_client.get_pool(name)
pre_state = existing  # None if it didn't exist; dict if it did

# ... do the write ...

# Save pre_state alongside the step record in the database
step_repo.save(step_id=..., pre_state_json=json.dumps(pre_state), ...)
```

Rollback compensation logic — every step needs a `compensate()` method:
```python
async def compensate(self, pre_state: dict | None, intent: dict):
    name = intent["name"]

    if pre_state is None:
        # This step CREATED the object → safe to delete it
        existing = await self.f5_client.get_pool(name)
        if existing is not None:
            await self.f5_client.delete_pool(name)

    else:
        # This step MODIFIED a pre-existing object → restore it, NEVER delete
        await self.f5_client.update_pool(name, pre_state)
```

The rollback loop (reverse order, stop on first compensation failure):
```python
async def rollback(completed_steps: list[tuple[Step, StepResult]]):
    for step, result in reversed(completed_steps):
        try:
            await step.compensate(pre_state=result.pre_state, intent=intent)
        except Exception as exc:
            # Compensation itself failed — escalate immediately, never loop
            await mark_needs_attention(reason=f"rollback_failed at {step.name}: {exc}")
            await send_notification(...)
            return   # stop — further compensation could make things worse

    await mark_rolled_back()
```

**Why this rule:** A request tries to update a pool that already had 2 members. The WideIP
step that follows fails. Rollback must restore the pool to its original 2-member state — not
delete the pool. Deleting would break every WideIP that was already using that pool.

### Enhancement C — Per-device concurrency semaphore

**Why F5 mcpd is the bottleneck:** F5's `mcpd` daemon serialises all configuration writes.
It does not matter how many concurrent API calls you send — they queue inside mcpd and
are applied one at a time. The real limit is not network throughput; it is mcpd's
configuration transaction rate. Running 20 concurrent workflows per device does not make
things 20× faster — it queues 20 config saves and causes mcpd to report timeouts and errors.

Add a per-device counting semaphore in Redis to cap concurrent workflows at P-1 (a number
you must measure from your specific device — do not guess).

All Redis operations for the semaphore must be atomic Lua scripts. If you read the slot
count and then write in separate Python calls, two pods will both read "9/10 slots used"
and both acquire, resulting in 11 concurrent workflows on a device that allows 10.

**Lua script — semaphore_acquire.lua:**
```lua
-- KEYS[1] = "sem:{device_id}"
-- ARGV[1] = max_slots, ARGV[2] = slot_ttl_seconds, ARGV[3] = worker_id
local key      = KEYS[1]
local max_slots = tonumber(ARGV[1])
local ttl      = tonumber(ARGV[2])
local worker   = ARGV[3]

local count = redis.call('HLEN', key)
if count < max_slots then
    redis.call('HSET', key, worker, tostring(redis.call('TIME')[1]))
    redis.call('EXPIRE', key, ttl)
    return 1  -- acquired
end
return 0  -- full, try again later
```

**Lua script — semaphore_release.lua:**
```lua
redis.call('HDEL', KEYS[1], ARGV[1])
return 1
```

**Python class:**
```python
class DeviceSemaphore:
    def __init__(self, redis_client, device_id, max_slots, slot_ttl):
        self._redis = redis_client
        self._key = f"sem:{device_id}"
        self._max_slots = max_slots   # P-1: measure from load test, never guess
        self._slot_ttl = slot_ttl     # set to 3 × heartbeat_interval

    async def acquire(self, worker_id: str, timeout_seconds: float) -> bool:
        deadline = time.monotonic() + timeout_seconds
        poll_interval = 0.1
        while True:
            result = await self._redis.eval(
                ACQUIRE_LUA, 1, self._key,
                self._max_slots, self._slot_ttl, worker_id
            )
            if result:
                return True
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(min(poll_interval, deadline - time.monotonic()))
            poll_interval = min(poll_interval * 1.5, 2.0)   # backoff, cap at 2s

    async def release(self, worker_id: str):
        await self._redis.eval(RELEASE_LUA, 1, self._key, worker_id)

    async def renew(self, worker_id: str, new_ttl: int):
        # Called by heartbeat to keep the slot alive
        await self._redis.expire(self._key, new_ttl)

    @asynccontextmanager
    async def slot(self, worker_id: str, timeout_seconds: float):
        acquired = await self.acquire(worker_id, timeout_seconds)
        try:
            yield acquired
        finally:
            if acquired:
                await self.release(worker_id)
```

**In the worker:**
```python
async with semaphore.slot(worker_id, timeout_seconds=P4_SEMAPHORE_TIMEOUT) as acquired:
    if not acquired:
        # Revert to QUEUED, re-enqueue with backoff, exit
        await revert_to_queued(request_id)
        return

    heartbeat_task = asyncio.create_task(run_heartbeat(request_id, worker_id))
    try:
        await run_all_steps(...)
    finally:
        heartbeat_task.cancel()
        await asyncio.gather(heartbeat_task, return_exceptions=True)
```

**Self-healing:** The semaphore key has a TTL equal to `slot_ttl`. The heartbeat renews
this TTL every P-5 seconds. If a worker pod is killed, the heartbeat stops, the TTL ticks
down, and the slot disappears automatically — no manual cleanup, no stale slots.

### Enhancement D — Heartbeat

The heartbeat does two things simultaneously:
1. Updates `last_heartbeat_at` in the database (proves the worker is alive)
2. Renews the semaphore slot TTL in Redis (prevents auto-expiry)

Both must happen together. If you only do the DB update, the semaphore slot expires and
another worker steals it. If you only do the Redis renewal, the reclaim sweeper eventually
kills the "stale" DB row while the worker is still running.

```python
async def run_heartbeat(request_id, worker_id, semaphore, interval_seconds):
    while True:
        await asyncio.sleep(interval_seconds)
        try:
            # 1. DB
            await db.execute(
                "UPDATE requests SET last_heartbeat_at = ? WHERE request_id = ?",
                [datetime.utcnow(), request_id]
            )
            # 2. Redis semaphore slot TTL
            await semaphore.renew(worker_id, new_ttl=int(interval_seconds * 3))
        except Exception as exc:
            log.warning("heartbeat_error", error=str(exc))
            # Do not raise — a failed heartbeat tick is not fatal; the next one will try again
```

**Reclaim sweeper rule:** Only reclaim a RUNNING request when `last_heartbeat_at` is older
than `3 × heartbeat_interval`. A slow-but-healthy worker (long F5 call, healthy heartbeat)
must NEVER be reclaimed. Reclaiming it creates two concurrent workers on the same WideIP.

### Enhancement E — Token bucket rate limiter per device

**Why you need this in addition to the semaphore:** The semaphore caps concurrent
*workflows*. But one workflow makes multiple HTTP calls (pre-read, write, post-read for
each of 5 objects = up to 15 calls). If P-1 = 8 workflows, that is up to 120 concurrent
HTTP calls to one F5 device. The token bucket caps the *rate* of those HTTP calls,
keeping mcpd below saturation.

Every HTTP call to F5 must consume one token before it is made.

**Lua script — token_bucket.lua (must be atomic):**
```lua
-- KEYS[1] = "bucket:{device_id}"
-- ARGV[1]=capacity, ARGV[2]=refill_rate (per second), ARGV[3]=tokens_requested, ARGV[4]=now
local capacity     = tonumber(ARGV[1])
local refill_rate  = tonumber(ARGV[2])
local requested    = tonumber(ARGV[3])
local now          = tonumber(ARGV[4])

local data        = redis.call('HMGET', KEYS[1], 'tokens', 'last_refill')
local tokens      = tonumber(data[1]) or capacity
local last_refill = tonumber(data[2]) or now

local elapsed    = math.max(0, now - last_refill)
local new_tokens = math.min(capacity, tokens + elapsed * refill_rate)

if new_tokens >= requested then
    redis.call('HMSET', KEYS[1], 'tokens', new_tokens - requested, 'last_refill', now)
    return 1   -- allowed
else
    redis.call('HMSET', KEYS[1], 'tokens', new_tokens, 'last_refill', now)
    return 0   -- rejected
end
```

**Python wrapper:**
```python
class DeviceTokenBucket:
    def __init__(self, redis_client, device_id, capacity, refill_rate):
        self._redis = redis_client
        self._key = f"bucket:{device_id}"
        self._capacity = capacity      # P-2: burst ceiling — measure from load test
        self._refill_rate = refill_rate  # P-3: tokens per second — measure from load test

    async def consume(self) -> bool:
        result = await self._redis.eval(
            TOKEN_BUCKET_LUA, 1, self._key,
            self._capacity, self._refill_rate, 1, time.time()
        )
        return bool(result)
```

**Wire it before every F5 HTTP call:**
```python
async def call_f5(self, method, path, body=None):
    allowed = await self._token_bucket.consume()
    if not allowed:
        raise RateLimitError("Token bucket empty — too many requests to this device")
    # proceed with HTTP
    return await self._session.request(method, path, json=body)
```

**Why Lua for this (and for the semaphore):** Any read-then-write in Python is a race
condition when 4 pods run concurrently. Example: pod A reads "5 tokens left", pod B reads
"5 tokens left", both consume 1, both write "4 tokens left". You have effectively consumed
1 token instead of 2. Multiply by 4 pods: your rate limiter does nothing. Lua scripts
execute atomically inside Redis — no other command runs between the read and the write.

### Enhancement F — Circuit breaker per device

**Why this is critical with 4 devices:** Without a circuit breaker, if one F5 device goes
down, its entire queue of requests tries to run, each one times out after 30+ seconds,
and the workers are blocked on that device while the other 3 devices sit idle. The circuit
breaker detects the failing device and stops sending it work, keeping the other 3 devices
healthy.

**Three states:**
- `CLOSED` — normal, requests flow through
- `OPEN` — device known-bad, new requests queued but not dispatched, existing queue held
- `HALF_OPEN` — one probe request allowed; success → CLOSED; failure → back to OPEN

**Three signals that open the breaker:**
- Error rate in a 60-second sliding window exceeds threshold (e.g. >20%)
- p95 latency in the same window exceeds threshold (e.g. >5000ms)
- N consecutive timeouts (e.g. 3 in a row)

All thresholds are configurable — never hardcoded. Start with conservative values.

**After every F5 call, record the outcome:**
```python
import time

t0 = time.monotonic()
try:
    response = await call_f5(method, path, body)
    latency_ms = (time.monotonic() - t0) * 1000
    await breaker.record_success(latency_ms)
    return response
except asyncio.TimeoutError:
    latency_ms = (time.monotonic() - t0) * 1000
    await breaker.record_timeout()            # timeout ≠ failure — outcome unknown
    raise F5TimeoutError(...)
except F5Error:
    latency_ms = (time.monotonic() - t0) * 1000
    await breaker.record_failure(latency_ms)
    raise
```

**At admission (API layer):** check breaker state for the target device. If OPEN, return
503 + Retry-After. Do not accept work for a known-failing device.

**Breaker state must live in Redis**, not in-process. With 4 pods, in-process state means
3 pods don't know the device is failing. Shared Redis means all pods see the same state
within one polling interval.

**Timeout rule — the most important:** A timeout from F5 means the outcome is UNKNOWN.
The config change may have been applied or it may not. Never blind-retry a timeout —
you will create duplicates or apply conflicting changes. Instead:
1. Record as a timeout in the breaker (count toward consecutive timeout threshold)
2. Read back the current state of the object from F5
3. If the desired state is already applied → treat as success
4. If the object is in an intermediate state → decide whether to continue or roll back

### Enhancement G — Queue depth admission control

At the API level, before accepting a new request, check:
1. Is the global count of queued/running requests below the global limit (P-7)?
2. Is the per-device count below the per-device limit (P-8)?

If either check fails → 503 + `Retry-After` header. Do not add the request to the queue.

Implementation:
```python
# At API admission
global_depth = int(await redis.get("queue_depth:global") or 0)
device_depth = int(await redis.get(f"queue_depth:{device_id}") or 0)

if global_depth >= P7_GLOBAL_LIMIT:
    raise HTTPException(503, "Global queue full", headers={"Retry-After": "30"})
if device_depth >= P8_DEVICE_LIMIT:
    raise HTTPException(503, "Device queue full", headers={"Retry-After": "30"})

# After successful enqueue to Celery:
await redis.incr("queue_depth:global")
await redis.incr(f"queue_depth:{device_id}")

# When a request reaches COMPLETED or ROLLED_BACK:
await redis.decr("queue_depth:global")
await redis.decr(f"queue_depth:{device_id}")
```

**Fail closed:** If Redis is unreachable at admission time, return 503 + Retry-After.
Do not proceed without this check. A Redis outage must not mean unlimited concurrency
at F5 — it means "slow down until Redis is back."

### Enhancement H — Operational controls (no redeploy required)

All flags live in Redis. They can be toggled live with a `redis-cli SET` command or an
admin API endpoint. Check them at the start of each workflow execution, not just at startup.

| Control | Redis key | Behaviour when active |
|---|---|---|
| Kill switch | `ops:kill_switch` | Halt all F5/Infoblox writes. In-flight requests park back in QUEUED. Nothing is lost. |
| Dry run | `ops:dry_run` | Log every intended call with full payload. Execute nothing externally. |
| Delete cap | `ops:delete_count:{window_key}` | Refuse more than N deletes per hour. N is a business decision. |
| Device disable | `ops:device_disabled:{device_id}` | Stop accepting work for one device. Other 3 unaffected. |

Check kill switch at the start of each step, not once at the start of the workflow. A kill
switch engaged mid-workflow should stop new steps but let the current step complete.

### Enhancement I — Notification polling endpoint

The consuming app calls this every 1 minute to check for failures.

```
GET /api/v1/notifications?since={ISO 8601 timestamp}&limit={int}
```

Response:
```json
{
  "needs_attention": [
    {
      "request_id": "...",
      "fqdn": "app1.example.com",
      "action": "create",
      "reason": "rollback_failed at WideIPStep: connection timeout",
      "since": "2026-08-17T10:05:00Z"
    }
  ],
  "rollback_failed": [...],
  "open_breakers": [
    { "device_id": "f5-dc-a-01", "state": "OPEN", "since": "..." }
  ],
  "remediation_escalated": [...],
  "summary": {
    "needs_attention_count": 1,
    "open_breakers_count": 1
  },
  "polled_at": "2026-08-17T10:06:00Z"
}
```

The consumer passes `polled_at` from the previous response as `since` on the next call.
This gives incremental results — only new events since the last poll.

`NEEDS_ATTENTION` is a terminal state. Nothing automatic exits it. The on-call team
resolves it manually. Every entry to this state must be visible here.

---

## Hard rules — apply to every line you write

**1. Never invent a P-n parameter value.**
Every number that governs load — concurrency limit, bucket capacity, refill rate, breaker
thresholds, queue limits, timeouts — must come from actual load measurements against your
F5 device. A wrong number here silently overloads mcpd. Leave placeholders with
`# TODO: measure from load test` comments. Do not substitute "reasonable-looking" numbers.

**2. Never assume an F5 or Infoblox API shape.**
Confirm every endpoint path, field name, and response format against F5 iControl REST
documentation for the exact BIG-IP version installed, or against a live dev call.
Field names and endpoint paths differ across versions. Do not write plausible-looking code
from memory — confirm it.

**3. Every F5 and Infoblox operation must be idempotent.**
Read current state → compare to desired → act only if different → no-op if identical →
never error on a second run of the same request. The no-op branch is mandatory.

**4. Rollback must never delete pre-existing objects.**
If `pre_state` is `None` → the object was created by this request → safe to delete.
If `pre_state` is a `dict` → the object existed before → restore it, never delete.

**5. Timeouts are unknown outcomes, not failures.**
Do not blind-retry after a timeout. Read back the object's current state first.

**6. Redis unavailable → fail closed.**
If Redis cannot be reached at admission time, return 503 + Retry-After. Never proceed.

**7. The API must never call F5 or Infoblox.**
All external calls happen in Celery workers. The API path must complete in milliseconds.

**8. Enqueue only the request_id, never the payload.**

**9. All Redis read-then-write operations must use Lua scripts.**
Semaphore acquire/release, token bucket consume, circuit breaker state updates — all Lua.

**10. Two concurrency guards are both required.**
(a) Unique index on `requests(wip_fqdn) WHERE status IN (active statuses)` — prevents
duplicate DB rows.
(b) Atomic `UPDATE ... WHERE request_id=? AND status='QUEUED'` claim in the worker —
prevents two workers running the same request.
Neither alone is sufficient. Both must be present.

---

## Failure scenarios — every one must be handled

| Scenario | Correct behaviour |
|---|---|
| Monitor created, pool call fails | Rollback monitor only if it did not pre-exist |
| WideIP created, CNAME (Infoblox) fails | Retry CNAME with exponential backoff → NEEDS_ATTENTION after N attempts |
| Worker pod killed mid-workflow | Heartbeat goes stale → reclaim sweeper re-queues after 3× heartbeat interval |
| Slow worker, healthy heartbeat | Never reclaim — a live heartbeat means the worker is still working |
| F5 returns 5xx | Retry with exponential backoff + jitter; count toward circuit breaker error rate |
| F5 timeout | Read back current state; record as timeout in breaker; do not blind-retry |
| Infoblox unreachable | Queue CNAME step for retry; leave F5 objects in place |
| DELETE, object already absent | Succeed silently (idempotent); do not raise an error |
| Two concurrent requests for same FQDN | Second request gets 409 + details of the running one |
| Rollback compensation fails | Mark NEEDS_ATTENTION; alert; stop — do not loop |
| Circuit breaker open | Park request as QUEUED; do not fail it; alert on-call |
| Redis OOM (noeviction hit) | API catches OOM error from `.delay()` and returns 503, not 500 |

---

## What to produce before starting implementation

Before writing any code, produce a short analysis document:

1. **What the existing code already does correctly** — list each behaviour
2. **What the existing code does that conflicts with these enhancements** — list each conflict and how you will resolve it
3. **What is missing entirely** — list each gap
4. **Order you will implement the enhancements** — justify the order

Stop after producing this analysis. Do not begin implementation until the analysis is reviewed.
