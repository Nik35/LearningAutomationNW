# Enhancement Specification — F5 GTM Automation Service

Paste this file as your first message to a new agent working on this codebase.
The agent has access to the existing code. This document tells it what to enhance and how.

---

## What this service does today

A FastAPI + Celery + Redis + MSSQL service that provisions GSLB configuration on
**F5 BIG-IP DNS rSeries (BIG-IP 17.1.x)** and **CNAME records in Infoblox** in response
to REST API calls from an OpenShift-based consumer.

**Core flow already working:**
- `POST /wideip`, `PUT /wideip`, `DELETE /wideip` — accept a request, call F5 and
  Infoblox to create/update/delete a WideIP (monitor → pool → pool members → WideIP → CNAME)
- `GET /wideip/{request_id}` — status polling
- Deployed across 2 datacentres, 4 pods, targeting 4 independent F5 grids
- Every OpenShift deployment depends on it — treat as production

**Confirmed facts you must not re-research:**

| Fact | Value |
|---|---|
| F5 platform | BIG-IP rSeries |
| BIG-IP version | 17.1.x |
| F5 auth mode | TACACS+ — `loginProviderName` in the login call must be set to the TACACS+ source name from the BIG-IP (run `tmsh list auth tacacs` on the device). Default `"tmos"` is wrong for this deployment. |
| GTM transactions | **Not supported** on this version. The 4 F5 steps (monitor, pool, members, wideip) remain separate API calls. |
| Infoblox CNAME fields | `name` and `canonical` required; `view`, `ttl`, `use_ttl`, `comment` optional (WAPI 2.13) |
| Redis eviction policy | Must be `noeviction`. Any `allkeys-*` policy silently deletes queued Celery tasks. |
| Notification delivery | Consuming app polls `GET /api/v1/notifications?since={ISO timestamp}` every 1 minute. No push or webhook. |

---

## What needs to be enhanced

The existing app handles the happy path. It is missing the protections that make it
safe for production under load, partial failure, and concurrent requests. These are
the enhancements to add, in priority order:

1. **Idempotency on every F5 and Infoblox call** — read current state → compare → act only if different → no-op if identical
2. **Rollback with pre-state capture** — if any step fails, undo completed steps in reverse; never delete objects that existed before the request
3. **Per-device concurrency semaphore** — cap how many workflows run simultaneously against one F5 device
4. **Heartbeat** — workers renew a timestamp while running; used to detect dead workers without killing slow ones
5. **Token bucket rate limiter** — cap the rate of outbound F5 HTTP calls per device
6. **Circuit breaker** — detect a failing F5 device and stop sending it work until it recovers
7. **Queue depth admission control** — reject new requests at the API when queues are already full
8. **Operational controls** — kill switch, dry-run mode, delete cap — all togglable at runtime without redeploy
9. **Notification polling endpoint** — `GET /api/v1/notifications?since=` for the consumer to check for failures

---

## The complete request lifecycle (with all enhancements)

This is the target state. Implement each section in the order shown.

```
──────────────────────────────────────────────────────────────────
API PATH  (must complete in milliseconds — no F5 or Infoblox calls)
──────────────────────────────────────────────────────────────────

1. Receive POST/PUT/DELETE /wideip
2. Validate payload schema  →  400 on failure
3. Resolve target_device    →  400 if unknown
4. Compute idempotency_key = sha256(action | wip_fqdn | normalise(payload))
     normalise = sort keys, lowercase FQDNs, strip whitespace

5. ADMISSION CHECKS — cheapest first, fail closed:
     a. Is Redis reachable?                     → 503 + Retry-After if not
     b. Is kill switch engaged?                 → 503 + Retry-After if yes
     c. Is global queue depth ≥ P-7?            → 503 + Retry-After if yes
     d. Is circuit breaker OPEN for this device
        AND device queue depth ≥ P-8?           → 503 + Retry-After if yes

6. Atomic DB insert guarded by partial unique index on active wip_fqdn:
     - Success      → row written, status = RECEIVED
     - Duplicate key → SELECT existing row:
         same idempotency_key  → 200 + original request_id  (idempotent replay)
         different key         → 409 + running request details (conflict)

7. Transition RECEIVED → QUEUED
8. Enqueue Celery task with request_id ONLY (never the payload)
9. Increment queue depth counters in Redis: queue_depth:global, queue_depth:{device_id}
10. Return 202 { request_id, status, status_url, retry_after }

──────────────────────────────────────────────────────────────────
WORKER PATH  (runs in Celery)
──────────────────────────────────────────────────────────────────

1. Load request from DB by request_id

2. Atomic DB claim:
     UPDATE requests SET status='RUNNING', worker_id=?, started_at=NOW()
     WHERE request_id=? AND status='QUEUED'
   → 0 rows affected = another worker owns it → exit silently

3. Acquire per-device semaphore slot (timeout P-4):
     → timeout → revert to QUEUED, re-enqueue with backoff, exit

4. Start heartbeat background task:
     Every P-5 seconds: UPDATE requests SET last_heartbeat_at=NOW()
     AND renew semaphore slot TTL
     Stops when workflow completes or fails

5. PRE-CHECKS:
     - Kill switch active? → revert to QUEUED, release slot, exit
     - Transition RUNNING → VERIFYING

6. EXECUTE STEPS in order (create: monitor→pool→members→wideip→cname;
                            delete: cname→wideip→members→pool→monitor):

   For each step:
     a. Write request_steps row: intent_json, status=PENDING
     b. READ current state from F5 / Infoblox
     c. SAVE pre_state  ← critical for rollback
     d. COMPARE desired vs actual:
          - identical → mark SUCCEEDED, continue (no-op — this is idempotency)
          - different → call F5 / Infoblox to create/update/delete
     e. Token bucket: consume 1 token before every outbound HTTP call
          → bucket empty → raise error → triggers rollback
     f. After HTTP call: record outcome in circuit breaker
          → success + latency → record_success(latency_ms)
          → timeout          → record_timeout()
          → other error      → record_failure(latency_ms)
     g. Mark step SUCCEEDED, save result_json

   If any step fails → ROLLBACK (see below)

7. POST-VALIDATION:
     Read back each object from F5 / Infoblox, compare to intent
     Mismatch → VERIFY_FAILED → remediation queue

8. Mark COMPLETED
9. Decrement queue depth counters: queue_depth:global, queue_depth:{device_id}

FINALLY (always runs, even on unhandled exception):
     - Cancel heartbeat task
     - Release semaphore slot

──────────────────────────────────────────────────────────────────
ROLLBACK PATH  (triggered when any step fails)
──────────────────────────────────────────────────────────────────

Mark ROLLING_BACK

For each completed step in REVERSE order:
     IF pre_state is None:
          object was created by this request → safe to delete
     IF pre_state is a dict:
          object existed before → restore prior state, NEVER delete it

     If compensation fails → NEEDS_ATTENTION + send notification → stop
     (never loop on rollback failure)

All compensations succeeded → mark ROLLED_BACK
Decrement queue depth counters
```

---

## Enhancement 1 — Idempotency (every F5 and Infoblox operation)

**Rule:** Read current state → compare to desired → act only if different → no-op if identical → never error on second run.

**Pattern for every create/update operation:**
```
existing = GET /mgmt/tm/gtm/wideip/a/~partition~{name}
if existing is None:
    POST to create
elif existing differs from desired:
    PATCH to update
else:
    return no_op  ← this branch is mandatory
```

**Pattern for every delete operation:**
```
existing = GET /mgmt/tm/gtm/wideip/a/~partition~{name}
if existing is None:
    return no_op  ← already gone, succeed silently
else:
    DELETE it
```

The no-op branch is the most commonly omitted piece. Its absence turns every retry into an error and every re-run into a duplicate.

---

## Enhancement 2 — Rollback with pre-state capture

Before every write to F5 or Infoblox, save the current state:
- Object does not exist → `pre_state = None`
- Object exists → `pre_state = <full object dict from the GET response>`

On rollback, apply in reverse step order:
- `pre_state is None` → object was new → delete it
- `pre_state is dict` → object existed before → PUT/PATCH it back to pre_state, never delete

**Why this rule exists:** A failed PUT must not destroy an object that was there before this request arrived. Example: a pool existed with 2 members. The request tried to add a 3rd member and the next step (WideIP) failed. Rollback must restore the pool to 2 members — not delete the pool.

**Code — how to capture pre_state before every write:**
```python
class StepResult:
    def __init__(self, action, pre_state, post_state):
        self.action = action        # "created" | "updated" | "no_op" | "deleted" | "not_found"
        self.pre_state = pre_state  # None = did not exist before; dict = existed before
        self.post_state = post_state

# In each step's execute() method:
async def execute(self, intent: dict) -> StepResult:
    # 1. Read current state
    existing = await self.f5_client.get_pool(intent["name"])

    # 2. Save pre_state BEFORE any write
    pre_state = existing  # None if pool doesn't exist, dict if it does

    if existing is None:
        # Create
        await self.f5_client.create_pool(intent)
        post_state = await self.f5_client.get_pool(intent["name"])
        return StepResult("created", pre_state=None, post_state=post_state)

    elif self._differs(existing, intent):
        # Update
        await self.f5_client.update_pool(intent["name"], intent)
        post_state = await self.f5_client.get_pool(intent["name"])
        return StepResult("updated", pre_state=existing, post_state=post_state)

    else:
        # No-op — already matches intent
        return StepResult("no_op", pre_state=existing, post_state=existing)
```

**Code — the rollback loop (runs in reverse order):**
```python
async def rollback(completed_steps: list[tuple[Step, StepResult]]):
    for step, result in reversed(completed_steps):
        try:
            await step.compensate(result.pre_state, intent)
        except Exception as exc:
            # Rollback itself failed → escalate immediately, never loop
            mark_needs_attention(reason=f"rollback failed at {step.name}: {exc}")
            send_notification(...)
            return

async def compensate(self, pre_state, intent):
    if pre_state is None:
        # This step created the object → safe to delete
        await self.f5_client.delete_pool(intent["name"])
    else:
        # This step modified a pre-existing object → restore it, NEVER delete
        await self.f5_client.update_pool(intent["name"], pre_state)
```

**Real example — what rollback looks like for a failed WideIP step:**
```
Steps completed before failure:
  1. MonitorStep  → created new monitor  (pre_state=None)
  2. PoolStep     → updated existing pool (pre_state={"name":"pool1","members":[...]})
  3. WideIPStep   → FAILED (error from F5)

Rollback runs in reverse:
  2. PoolStep.compensate(pre_state={"name":"pool1",...})
       → pre_state is dict → PATCH pool back to original state   ✓
  1. MonitorStep.compensate(pre_state=None)
       → pre_state is None → DELETE the monitor we created       ✓

Result: system is back to its state before the request arrived.
```

---

## Enhancement 3 — Per-device semaphore

**What it does:** Limits concurrent workflows per F5 device to P-1. F5's config-write throughput is finite (mcpd serialises config saves). Running unlimited concurrent workflows overloads the device even if each call looks fast individually.

**Algorithm:**
- Redis Hash key: `sem:{device_id}`
- Each field: `worker_id → acquired_at_timestamp`
- Entire hash has a TTL (= P-5 × 3 seconds)

**Acquire** (atomic Lua, never Python read-modify-write):
```
count = HLEN sem:{device_id}
if count < max_slots:
    HSET sem:{device_id} {worker_id} {now}
    EXPIRE sem:{device_id} {ttl}
    return GRANTED
else:
    return FULL
```

**Release:** `HDEL sem:{device_id} {worker_id}`

**Renew (heartbeat):** `EXPIRE sem:{device_id} {ttl}` — resets the TTL so a live worker's slot doesn't expire

**Self-healing:** If a worker dies, the heartbeat stops, the TTL ticks down, and the slot disappears automatically. No manual cleanup needed.

**Acquire timeout (P-4):** Poll every 100ms with exponential backoff up to 2s. If P-4 elapses without a slot → revert request to QUEUED, re-enqueue with backoff.

---

## Enhancement 4 — Heartbeat

**What it does:** Proves a worker is still alive. The reclaim sweeper only reclaims RUNNING requests whose `last_heartbeat_at` is older than P-6 (= 3 × P-5). A slow-but-healthy worker is never reclaimed.

**Implementation:** Background async task inside the worker. Every P-5 seconds:
1. `UPDATE requests SET last_heartbeat_at = NOW() WHERE request_id = ?`
2. `EXPIRE sem:{device_id} {slot_ttl}` — keep semaphore slot alive too

Always stop the heartbeat in a `finally` block when the workflow ends.

---

## Enhancement 5 — Token bucket rate limiter

**What it does:** Caps the rate of outbound HTTP calls to a specific F5 device. The semaphore limits *concurrent* workflows; the token bucket limits the *rate* of actual HTTP requests regardless of how many workflows are running.

**Algorithm (atomic Lua):**
- Redis Hash key: `bucket:{device_id}`
- Fields: `tokens` (current count), `last_refill` (Unix timestamp)

```lua
elapsed = now - last_refill
new_tokens = min(capacity, tokens + elapsed * refill_rate)
if new_tokens >= 1:
    store new_tokens - 1, now
    return ALLOWED
else:
    store new_tokens, now
    return REJECTED
```

**Where to call it:** Before every outbound HTTP call to F5. If rejected → raise an error immediately (do not queue or wait — the semaphore already handles queuing).

**Parameters:** P-2 = bucket capacity (burst ceiling), P-3 = refill rate per second. Both come from WP-0 load measurements. Leave as placeholders until measured.

**Lua script (token_bucket.lua) — must be atomic, one script, no Python read-modify-write:**
```lua
-- KEYS[1] = "bucket:{device_id}"
-- ARGV[1] = capacity, ARGV[2] = refill_rate, ARGV[3] = tokens_requested, ARGV[4] = now (unix float)
local capacity      = tonumber(ARGV[1])
local refill_rate   = tonumber(ARGV[2])
local requested     = tonumber(ARGV[3])
local now           = tonumber(ARGV[4])

local data       = redis.call('HMGET', KEYS[1], 'tokens', 'last_refill')
local tokens     = tonumber(data[1]) or capacity
local last_refill = tonumber(data[2]) or now

-- add tokens based on time elapsed since last call
local elapsed   = math.max(0, now - last_refill)
local new_tokens = math.min(capacity, tokens + elapsed * refill_rate)

if new_tokens >= requested then
    redis.call('HMSET', KEYS[1], 'tokens', new_tokens - requested, 'last_refill', now)
    return 1  -- allowed
else
    redis.call('HMSET', KEYS[1], 'tokens', new_tokens, 'last_refill', now)
    return 0  -- rejected (bucket empty)
end
```

**Python wrapper — called before every F5 HTTP call:**
```python
class DeviceTokenBucket:
    def __init__(self, redis_client, device_id, capacity, refill_rate):
        self._redis = redis_client
        self._key = f"bucket:{device_id}"
        self._capacity = capacity        # P-2: placeholder -1 until load test
        self._refill_rate = refill_rate  # P-3: placeholder -1 until load test

    async def consume(self) -> bool:
        """Returns True if allowed, False if bucket is empty."""
        import time
        result = await self._redis.eval(
            LUA_TOKEN_BUCKET_SCRIPT,
            1, self._key,
            self._capacity, self._refill_rate, 1, time.time()
        )
        return bool(result)

# In the F5 client, before every HTTP call:
async def _call_f5(self, method, path, body=None):
    allowed = await self._token_bucket.consume()
    if not allowed:
        raise RateLimitError(f"Token bucket empty for device {self._device_id}")
    # proceed with HTTP call
    response = await self._session.request(method, path, json=body)
    return response
```

**Real example — what this looks like for a pool creation call:**
```
Request 1  arrives → bucket has 10 tokens → consume 1 → 9 remaining → F5 call proceeds
Request 2  arrives → bucket has 9  tokens → consume 1 → 8 remaining → F5 call proceeds
...
Request 11 arrives → bucket has 0  tokens → REJECTED  → step fails  → rollback triggered
```
If P-3 = 2 tokens/second, by the time request 11 arrives ~5 seconds later, the bucket has refilled to 10 and the call goes through.

---

## Enhancement 6 — Circuit breaker

**What it does:** Detects when an F5 device is consistently failing and stops sending it work until it recovers. Without this, one failing device blocks its entire queue while the other 3 devices are fine.

**Three states:**
- `CLOSED` — normal, requests flow through
- `OPEN` — device known-bad, new requests get 503 immediately and stay QUEUED
- `HALF_OPEN` — one probe request allowed; if it succeeds → CLOSED, if it fails → OPEN

**Three signals that open the breaker (all stored in Redis, visible to all pods):**
- Error rate in sliding window > P-10 threshold (e.g. >20% failures in last 60s)
- p95 latency in sliding window > P-10 threshold (e.g. >5000ms)
- N consecutive timeouts > P-10 threshold

**After every F5 HTTP call, record the outcome:**
- Success + latency → feeds into error rate and p95 calculation
- Timeout → increments consecutive timeout counter
- Other error → increments error counter

**At admission (API path):** Read breaker state for the target device. If OPEN → 503.

**Cross-pod:** Breaker state lives in Redis, so all pods see the same state. This is why Redis must be a shared instance (not per-pod).

**Timeout rule:** A timeout means the outcome is **unknown**. Never blind-retry after a timeout. Read back to determine actual state, then converge. Record it as a timeout in the breaker, not a failure.

---

## Enhancement 7 — Queue depth admission control

**What it does:** Rejects new requests at the API when there are already too many requests waiting or running, before they consume any worker capacity.

**Implementation:**
- Two Redis counters: `queue_depth:global` and `queue_depth:{device_id}`
- Increment both when a request is accepted and enqueued (best-effort, do not block the 202)
- Decrement both when a request reaches a terminal state (COMPLETED or ROLLED_BACK)
- At admission: read both counters and compare to P-7 (global limit) and P-8 (per-device limit)

**Fail closed:** If Redis is unreachable, reject the request with 503 + Retry-After. Never proceed without these checks — a Redis outage must not mean unlimited concurrency.

---

## Enhancement 8 — Operational controls (runtime toggleable, no redeploy)

All flags live in Redis. Toggle them live with a Redis SET command.

| Flag | Key | What it does |
|---|---|---|
| Kill switch | `ops:kill_switch` | Halts all F5/Infoblox writes. In-flight requests park back in QUEUED and are re-processed when the switch is cleared. Nothing is lost. |
| Dry run | `ops:dry_run` | Compute full workflow, log every intended call with payload, execute nothing. |
| Delete cap | `ops:delete_count:{window}` | Refuses more than P-9 deletes per rolling hour. |
| Device disable | `ops:device_disabled:{device_id}` | Removes one device from rotation for maintenance. Other devices unaffected. |

Check kill switch and dry-run at the start of each step execution, not just once at workflow start.

---

## Enhancement 9 — Notification polling endpoint

**Endpoint:** `GET /api/v1/notifications?since={ISO 8601 timestamp}&limit={int}`

**Called by:** The consuming OpenShift app, every 1 minute.

**Returns:**
```json
{
  "needs_attention": [
    { "request_id": "...", "fqdn": "...", "reason": "...", "since": "..." }
  ],
  "rollback_failed": [...],
  "open_breakers": [
    { "device_id": "...", "state": "OPEN", "since": "..." }
  ],
  "remediation_escalated": [...],
  "summary": { "needs_attention_count": 1, "open_breakers_count": 0 },
  "polled_at": "2026-08-17T10:00:00Z"
}
```

**Usage pattern:** Consumer passes `polled_at` from the previous response as `since` on the next call — incremental results only.

---

## Hard rules — apply to every line you write

**1. Never invent a P-n parameter value.**
Every load-governing number — concurrency limit, bucket size, refill rate, breaker thresholds, queue limits, timeouts — comes from load measurements against a live system. Until those measurements are done, every P-n field in config stays at its placeholder value (`-1` or `0.0`) with a `# TODO: awaiting load measurements` comment. A wrong value silently overloads production F5 devices.

**2. Never assume an F5 or Infoblox API shape.**
Confirm every endpoint, field name, and response format against F5 iControl REST docs for BIG-IP 17.1.x or against a live dev call. Field names differ across versions. Do not write plausible-looking API code from memory.

**3. Every F5 and Infoblox operation must be idempotent.**
Read current state → compare to desired → act only if different → no-op if identical → never error on the second run. The no-op branch is mandatory, not optional.

**4. Rollback must never delete pre-existing objects.**
Capture pre_state before every write. `pre_state is None` → safe to delete on rollback. `pre_state is dict` → restore prior state on rollback, never delete. A failed update must not destroy something that was there before the request arrived.

**5. Timeouts are unknown outcomes, not failures.**
A timeout from F5 or Infoblox means the call may have succeeded or failed — you do not know. Never blind-retry. Read back to determine actual state, then converge. Record it as a timeout in the circuit breaker.

**6. Redis unavailable → fail closed.**
If Redis cannot be reached at admission time, reject the new request with 503 + Retry-After. Never proceed without the coordination layer — a Redis outage must not mean unlimited concurrency at F5.

**7. The API never calls F5 or Infoblox.**
All external work happens in Celery workers. The API path must complete in milliseconds.

**8. Enqueue request_id only, never the payload.**
Pass only `request_id` (UUID string) and `device_id` to the Celery task. Never pass the payload. Passing payloads multiplies Redis memory usage per queued item.

**9. All Redis operations that read-then-write must be atomic Lua scripts.**
Python-level read-modify-write is a race condition when 4 pods run simultaneously. Use `redis.eval()` with a Lua script for all semaphore, token bucket, and circuit breaker operations. No exceptions.

**10. Two concurrency guards are both required.**
(a) Partial unique index on `requests(wip_fqdn) WHERE status IN (active states)` — prevents duplicate DB rows.
(b) Atomic `UPDATE ... WHERE status='QUEUED'` claim in the worker — prevents two workers running the same request.
Neither alone is sufficient.

---

## Object ordering — mandatory

```
CREATE:  monitor → pool → pool members → WideIP → CNAME
DELETE:  CNAME → WideIP → pool members → pool → monitor
```

CNAME must be removed **before** WideIP on delete. If WideIP is removed first, the CNAME points at nothing and DNS breaks for all consumers immediately.

---

## State machine

```
RECEIVED → QUEUED → RUNNING → VERIFYING → COMPLETED
                        │           │
                        │           └→ VERIFY_FAILED → REMEDIATING
                        └→ ROLLING_BACK → ROLLED_BACK
                                └→ (rollback fails) → NEEDS_ATTENTION
```

`NEEDS_ATTENTION` is terminal. Nothing automatic exits it. On-call team resolves manually. Every entry to NEEDS_ATTENTION must be visible via the notification polling endpoint.

---

## Failure scenarios to handle

| Scenario | Correct behaviour |
|---|---|
| Monitor created, pool fails | Rollback monitor (only if monitor did not pre-exist) |
| WideIP created, CNAME fails | Retry CNAME with backoff → escalate to NEEDS_ATTENTION after N attempts |
| Worker dies mid-workflow | Heartbeat goes stale → sweeper reclaims after P-6 seconds → re-enqueue |
| Slow worker (healthy heartbeat) | Never reclaim — reclaiming would cause two workers on the same WideIP |
| F5 returns 5xx | Retry with exponential backoff + jitter; count toward circuit breaker |
| F5 timeout, outcome unknown | Read back to determine actual state; record timeout in breaker; never blind-retry |
| Infoblox unavailable | Queue CNAME for retry; F5 state stands |
| DELETE, object already gone | Succeed idempotently; do not error |
| Concurrent same-FQDN request | Reject second request with 409 + details of the running one |
| Rollback compensation fails | Mark NEEDS_ATTENTION; notify; never loop |
| Circuit breaker open | Park request in QUEUED; do not fail it; alert on-call |

---

## P-n parameters — never invent values

| ID | What it controls | How to derive |
|---|---|---|
| P-1 | Per-device concurrency (semaphore max slots) | Load test: find the concurrency at which F5 mcpd saturates |
| P-2 | Token bucket capacity per device (burst ceiling) | Load test: max safe burst before F5 degrades |
| P-3 | Token bucket refill rate per device (tokens/second) | Load test: sustained safe request rate |
| P-4 | Semaphore acquire timeout | Derived: expected max workflow duration × some buffer |
| P-5 | Heartbeat interval | Choose: 15–30 seconds suggested |
| P-6 | Stale heartbeat threshold | Derived: 3 × P-5 |
| P-7 | Global queue depth limit | Derived: acceptable backlog size before shedding load |
| P-8 | Per-device queue depth limit | Derived: per-device share of P-7 |
| P-9 | Max deletes per hour | Business decision — not a technical one |
| P-10 | Circuit breaker thresholds (error rate, p95 latency, consecutive timeouts) | Load test: what failure rate indicates a device is genuinely down |

Every P-n field in config must stay at `-1` or `0.0` with a `# TODO: awaiting load measurements` comment until the load tests are run.
