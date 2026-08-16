# Agent Handoff Prompt — F5 GTM Automation Service (Phase 2)

Paste this entire file as your first message to a new Claude Code agent.

---

## Context

You are continuing work on a **production F5 GTM automation service** at `e:\Training-Nikhil\F5 api\`.

This service provisions GSLB configuration on **F5 BIG-IP DNS rSeries (BIG-IP 17.1.x)** — WideIP, pool, pool members, monitor — and **CNAME records in Infoblox**, in response to API calls from an OpenShift-based consumer. Every OpenShift deployment depends on it. Treat it as production code.

### What exists today

Two layers of work have been done:

**Layer 1 — Your existing app (the prior implementation):**
The original service is already running in dev. It handles the core POST/PUT/DELETE flow for F5 and Infoblox. Read the existing code fully before touching anything.

**Layer 2 — The architectural enhancement (already scaffolded in this repo):**
A full implementation plan at `docs/gtm-automation-implementation-plan.md` and a new module scaffold has been built. The scaffold is at `app/` and covers: state machine, DB schema, Redis coordination primitives, F5/Infoblox clients, workflow engine, Celery tasks, API routes, recovery modules, and operational controls.

**Your job is to merge these two layers** — understand the existing app's actual behaviour, find where the scaffold diverges from or duplicates it, and integrate them into one robust service following the plan.

---

## Step 1 — Read everything before writing a single line

Read in this order:

1. `docs/gtm-automation-implementation-plan.md` — the authoritative spec
2. `docs/api-research-findings.md` — confirmed F5 and Infoblox API shapes (do not invent anything not in this file)
3. `docs/gap-analysis.md` — task-by-task status of what the scaffold has built and what remains
4. Every file in `app/` — understand the scaffold before modifying it
5. The existing app code (wherever it lives in this repo or is described to you) — understand what is already working in prod

---

## Step 2 — Produce a merge analysis before writing any code

Write `docs/merge-analysis.md` containing:

1. **What the existing app does that the scaffold does not yet do** — list each behaviour with file/line references
2. **What the scaffold adds that the existing app lacks** — list each with the relevant scaffold file
3. **Conflicts** — anywhere the scaffold's design contradicts the existing app's live behaviour. Flag each one; do not silently resolve.
4. **Safe integration order** — which scaffold modules can be enabled behind feature flags without touching existing working code, and in what order

Stop after producing this doc. Do not begin integration until the merge analysis is reviewed.

---

## Confirmed facts (do not re-research these)

| Fact | Value | Source |
|---|---|---|
| F5 platform | BIG-IP rSeries | Owner confirmed |
| BIG-IP version | 17.1.x | Owner confirmed — use this for any version-specific API behaviour |
| F5 auth mode | **TACACS+** | Owner confirmed. Set `F5_LOGIN_PROVIDER_NAME` env var to the TACACS+ auth source name from the BIG-IP (run `tmsh list auth tacacs` on the device to find the object name). The scaffold default `"tmos"` is wrong for this deployment. |
| Redis eviction policy | `noeviction` | Owner confirmed. `redis.conf` is in the repo root with the full required config. |
| GTM transactions | **NOT supported** | Confirmed from F5 CloudDocs. T-4.6 in the plan is cancelled. Four F5 steps remain separate calls. |
| Infoblox CNAME fields | `name`, `canonical` required; `view`, `ttl`, `use_ttl`, `comment` optional | Confirmed from WAPI 2.13 docs |
| Notification delivery | **Polling endpoint** | Consuming app calls `GET /api/v1/notifications?since={timestamp}` every 1 minute. No push/webhook. This endpoint exists at `app/api/notifications.py`. |

---

## Hard rules (apply for every line you write)

These are non-negotiable. Violating any of them silently breaks production.

**1. Never invent a P-n parameter value.**
Every load-governing number — concurrency limits, token bucket size and refill rate, breaker thresholds, queue depth limits, timeouts — comes from WP-0 measurements in `docs/gtm-automation-implementation-plan.md §9`. Until those measurements are supplied, every P-n field in `app/core/config.py` stays at its placeholder value (`-1` or `0.0`) with its `# TODO: awaiting T-0.x` comment. Do not replace placeholders with "reasonable-looking" numbers. A wrong number here silently overloads production F5 devices.

**2. Never assume an F5 or Infoblox API shape.**
`docs/api-research-findings.md` contains all confirmed endpoint paths, field names, and response formats. Use only what is in that file. If you need something not covered there, write a `# TODO: confirm against F5 docs for 17.1.x` comment and leave the code incomplete rather than guessing.

**3. Every operation must be idempotent.**
Read current state → compare to desired → act only if different → no-op if identical → never error on the second run. The no-op branch is mandatory, not optional. This is the single most commonly omitted piece and its absence turns every retry into an error.

**4. Rollback must never delete pre-existing objects.**
Capture `pre_state` before every write. On rollback: `pre_state is None` → object was created by this request → safe to delete. `pre_state is dict` → object existed before → restore prior state, never delete. A failed update must not destroy something that was there before the request arrived.

**5. Timeouts are not failures.**
A timeout from F5 or Infoblox means the outcome is **unknown**. Never blind-retry after a timeout. Read back to determine actual state, then converge. See `F5TimeoutError` and `InfobloxTimeoutError` in the client modules.

**6. The reconciler must not write anything.**
`app/recovery/reconciler.py` passes `write_enabled=False` and raises immediately if `True`. This is D-10 from the plan and is absolute. The estate has inherited drift from a previous Ansible implementation; an auto-deleting reconciler on first prod run is the worst possible failure mode.

**7. The API never calls F5 or Infoblox.**
All external work happens in Celery workers. The API path (§3.1 of the plan) must complete in milliseconds: validate → admit → claim DB row → enqueue `request_id` only → return 202.

**8. Enqueue `request_id` only, never the payload.**
See D-2 in the plan. `run_gtm_workflow.delay(request_id=str(uuid), device_id=str)` only. Passing the payload doubles Redis memory usage per queued item.

**9. Feature flags for all new behaviour.**
The system is live. Every new component is disabled by default and enabled explicitly via `app/ops/controls.py` Redis flags. No behaviour-changing changes take effect on deploy — only when the flag is set.

**10. Two concurrency guards, both required.**
(a) The partial unique index `UX_requests_active_wip` on `requests(wip_fqdn) WHERE status IN (active states)` — prevents duplicate DB rows.
(b) The atomic `UPDATE ... WHERE status='QUEUED'` claim in `app/db/claim.py::atomic_claim_queued` — prevents two workers running the same request. Both must be present. Neither alone is sufficient.

---

## What the scaffold has built (do not duplicate)

### Done — do not rewrite unless the merge analysis reveals a conflict

| Module | Files | What it does |
|---|---|---|
| State machine | `app/domain/states.py` | 15-state machine, VALID_TRANSITIONS, InvalidTransitionError |
| DB schema | `app/db/migrations/001_initial.sql` | MSSQL DDL: all 5 tables, partial unique index, CHECK constraint |
| Repositories | `app/db/repositories.py` | Raw pyodbc (no ORM), parameterised queries |
| Atomic claims | `app/db/claim.py` | atomic INSERT + atomic QUEUED→RUNNING claim |
| Idempotency | `app/api/idempotency.py` | sha256 of normalised payload |
| Redis semaphore | `app/coordination/semaphore.py` + Lua | Per-device slot tracking, TTL-based dead worker cleanup |
| Token bucket | `app/coordination/ratelimit.py` + Lua | Per-device rate limiting |
| Circuit breaker | `app/coordination/breaker.py` + Lua | Per-device, cross-pod state |
| Ops controls | `app/ops/controls.py` | Kill switch, dry-run, delete cap, device disable — all live in Redis |
| Ops status | `app/ops/status.py` | Full system health snapshot |
| F5 client | `app/clients/f5/` | session (httpx pool), auth (TACACS+ via configurable loginProviderName, token cached in Redis), gtm (monitor/pool/members/wideip with read→compare→act) |
| Infoblox client | `app/clients/infoblox/` | session (ibapauth cookie), records (CNAME ensure/delete) |
| Workflow engine | `app/workflow/engine.py` | §3.2 orchestration: claim → semaphore → heartbeat → steps → rollback → release |
| Steps | `app/workflow/steps/` | monitor, pool, pool_members, wideip, cname — all with compensate() |
| Celery tasks | `app/tasks/` | celery_app (noeviction-aware, prefetch=1), workflows, beat |
| FastAPI app | `app/api/routes.py`, `app/main.py` | POST/PUT/DELETE /wideip, GET /wideip/{id}, §3.1 exactly |
| Notification poll | `app/api/notifications.py` | GET /api/v1/notifications?since= — polled every 1 min by consumers |
| Recovery | `app/recovery/reclaim.py` | Stale-heartbeat reclaim (healthy workers never reclaimed) |
| Remediation | `app/recovery/remediation.py` | Exponential backoff retry queue → NEEDS_ATTENTION escalation |
| Reconciler | `app/recovery/reconciler.py` | Report-only drift detection; write_enabled=False enforced at construction |
| Config | `app/core/config.py` | All P-n params as named fields with placeholder values and TODO comments |
| Logging | `app/core/logging.py` | structlog, request_id context propagation |
| Metrics | `app/core/metrics.py` | 18 Prometheus metrics, all per-device |

### What is PARTIAL or MISSING (your primary focus)

| Gap | Detail |
|---|---|
| **T-4.5 post-validation** | After each step, the scaffold trusts the step's `result_json`. Full read-back and compare against intent is not wired. VERIFY_FAILED path not triggered. |
| **T-0.9 Redis policy** | Redis is confirmed `noeviction` but the policy has not been verified by running `CONFIG GET maxmemory-policy` against each instance. Verify before enabling in prod. |
| **TACACS+ loginProviderName** | `F5_LOGIN_PROVIDER_NAME` env var must be set to the TACACS+ source name from the BIG-IP. Default is `"tmos"` which is wrong for this deployment. Find it with `tmsh list auth tacacs`. |
| **T-6.4 incremental reconciler** | Oldest `last_verified_at` first ordering not implemented |
| **T-6.5 per-category flags** | Per-category enable flags in the reconciler not implemented |
| **T-5.3 notification wiring** | Notification sender in engine.py is None. The notification channel is the polling endpoint at `/api/v1/notifications` — no push needed. Wire NEEDS_ATTENTION entries to also write an alert row in a new `notifications` table or use the existing query pattern. |
| **T-8.1 concurrency tests** | Integration test stubs exist at `tests/integration/test_concurrency.py` but are not complete |
| **T-8.5 rollback tests** | Stub exists, not implemented |
| **Existing app integration** | The scaffold exists alongside (or instead of) the original app. Merge analysis required before integration. |
| **WP-0 measurements** | None of T-0.1 through T-0.10 have been done against a live system. No P-n value can be set until they are. |

---

## Object creation order (§3.4 of the plan — confirm against F5 17.1.x docs)

```
CREATE:  monitor → pool → pool members → WideIP → CNAME
DELETE:  CNAME → WideIP → pool members → pool → monitor
```

CNAME must be removed **before** WideIP on delete. If the WideIP is removed first, the CNAME points at nothing and DNS breaks for consumers immediately.

---

## State machine (§3.6 of the plan)

```
RECEIVED → VALIDATING → QUEUED → RUNNING → VERIFYING → COMPLETED
    │           │           │        │          │
    │           │           │        │          └→ VERIFY_FAILED → REMEDIATING
    │           │           │        └→ FAILED → ROLLING_BACK → ROLLED_BACK
    │           │           │                          └→ ROLLBACK_FAILED
    │           │           └→ CANCELLED
    │           └→ REJECTED
    └→ REJECTED
                                          any terminal failure ↓
                                              NEEDS_ATTENTION
```

NEEDS_ATTENTION is terminal. Nothing automatic exits it. Entry writes to the `notifications` query path. The on-call team resolves manually.

---

## How the 4 protection layers work — with code examples

This is the most important section for a next agent to understand. All 4 layers are already built in `app/coordination/`. The wiring across the request lifecycle is what a next agent needs to complete and verify.

---

### The 4 layers and what each one protects

Think of a request going through a funnel. The cheapest checks happen first (admission), the most expensive happen last (inside the worker). This is deliberate — you don't spend a semaphore slot on a request you were going to reject anyway.

```
API receives POST
       │
       ▼
[Layer 1] Queue depth check  ← cheapest: just reads two Redis integers
       │  "are we full globally and for this device?"
       │  NO → 503 immediately
       │
       ▼
[Layer 2] Circuit breaker check  ← "is this device known to be failing right now?"
       │  OPEN → 503 immediately (park in QUEUED if already accepted)
       │
       ▼
  202 returned, request_id enqueued to Celery
       │
       ▼
  Celery worker picks up the task
       │
       ▼
[Layer 3] Semaphore acquire  ← "is there a free concurrency slot for this device?"
       │  TIMEOUT → revert to QUEUED, re-enqueue with backoff
       │  ACQUIRED → hold slot for entire workflow duration
       │
       ▼
  For each F5 call inside a step:
       │
       ▼
[Layer 4] Token bucket consume  ← "have we sent too many requests to this F5 too fast?"
          EMPTY → raise F5Error immediately (caught by step, triggers rollback)
          OK → proceed with HTTP call
```

---

### Layer 1 — Queue depth counter (Redis INCR/DECR)

**What it does:** Tracks how many requests are currently queued or running, globally and per device.

**Where it lives:**
- Increment: `app/api/routes.py` — after a request is accepted and enqueued
- Decrement: `app/workflow/engine.py::_decrement_queue_depth()` — when a request reaches COMPLETED or ROLLED_BACK
- Read: `app/api/admission.py` — in the admission check

**Code — increment on accept (routes.py, after `.delay()`):**
```python
try:
    pipe = redis.pipeline()
    pipe.incr("queue_depth:global")
    pipe.incr(f"queue_depth:{target_device}")
    await pipe.execute()
except Exception:
    pass  # best-effort; don't block the 202
```

**Code — decrement on terminal state (engine.py):**
```python
async def _decrement_queue_depth(self, device_id: str) -> None:
    try:
        if self._redis_client is not None:
            pipe = self._redis_client.pipeline()
            pipe.decr("queue_depth:global")
            pipe.decr(f"queue_depth:{device_id}")
            await pipe.execute()
    except Exception as exc:
        log.warning("workflow.queue_depth_decrement_failed", error=str(exc))
```

**Code — read in admission (admission.py):**
```python
global_depth = int(await redis_client.get("queue_depth:global") or 0)
device_depth = int(await redis_client.get(f"queue_depth:{target_device}") or 0)
if global_depth >= global_queue_limit:     # P-7
    return AdmissionResult(allowed=False, status_code=503, ...)
if device_depth >= device_queue_limit:     # P-8
    return AdmissionResult(allowed=False, status_code=503, ...)
```

**Why best-effort on INCR but not on DECR?** The INCR happens at 202-return time when latency matters. If Redis is down at that point, the request was already written to DB and enqueued to Celery — not incrementing the counter is acceptable because the next 503 won't cause data loss. The DECR is also best-effort but lives in a `finally`-equivalent path in the worker.

---

### Layer 2 — Circuit breaker (sliding window, Redis state)

**What it does:** Detects when an F5 device is consistently failing and stops sending it more work. An open breaker causes new requests to get 503 at admission rather than being queued to fail.

**Three signals that trip the breaker (all configurable as P-10 parameters):**
- Error rate in a sliding window exceeds threshold (e.g. >20% failures in last 60s)
- p95 latency exceeds threshold (e.g. p95 > 5000ms in last 60s)
- N consecutive timeouts (e.g. 3 in a row)

**Three states:**
- `CLOSED` = normal, requests flow through
- `OPEN` = device known-bad, new requests get 503 immediately
- `HALF_OPEN` = one probe request allowed through; if it succeeds → CLOSED, fails → OPEN again

**Where it lives:**
- State machine: `app/coordination/breaker.py` + `app/coordination/scripts/breaker_record.lua`
- Admission check: `app/api/admission.py` calls `breaker.peek_state()`
- Outcome recording: `app/clients/f5/gtm.py` calls `breaker.record_success/failure/timeout()` after every HTTP call

**Code — how the F5 client records outcomes (gtm.py):**
```python
async def _get(self, path: str) -> dict:
    await self._consume_token()          # Layer 4 first
    t0 = time.monotonic()
    try:
        response = await self._session.get(path, token=await self._token_manager.get_token())
        latency_ms = (time.monotonic() - t0) * 1000
        await self._record_outcome(latency_ms)     # success
        return response
    except asyncio.TimeoutError:
        latency_ms = (time.monotonic() - t0) * 1000
        await self._record_outcome(latency_ms, timed_out=True)
        raise F5TimeoutError(...)
    except Exception:
        latency_ms = (time.monotonic() - t0) * 1000
        await self._record_outcome(latency_ms, failed=True)
        raise

async def _record_outcome(self, latency_ms, *, timed_out=False, failed=False):
    if self._circuit_breaker is None:
        return
    if timed_out:
        await self._circuit_breaker.record_timeout()
    elif failed:
        await self._circuit_breaker.record_failure(latency_ms)
    else:
        await self._circuit_breaker.record_success(latency_ms)
```

**Why cross-pod state in Redis?** We have 4 pods across 2 DCs. If one pod sees an F5 device failing and opens its local breaker, the other 3 pods don't know. They keep sending requests to the failing device. By keeping breaker state in Redis, all pods see the same state within one poll interval. This is the only reason D-1 (shared Redis) exists.

---

### Layer 3 — Semaphore (counting, TTL-based self-healing)

**What it does:** Limits how many workflows can be running concurrently for a single F5 device. If P-1 slots are all taken, new workers wait until one frees up. If a worker dies without releasing its slot, the TTL expires and the slot is automatically reclaimed.

**Why it's needed separately from the queue depth:** The queue depth counter controls how many requests can be *waiting*. The semaphore controls how many can be *running simultaneously*. The F5 device has a finite config-write throughput (mcpd serialises config saves). Running 20 concurrent workflows against one device would overwhelm it even if the queue is within limits.

**Where it lives:**
- Implementation: `app/coordination/semaphore.py`
- Lua scripts: `app/coordination/scripts/semaphore_acquire.lua`, `semaphore_release.lua`, `semaphore_renew.lua`
- Used in: `app/workflow/engine.py::execute()` — wraps the entire workflow

**Code — how the engine uses the semaphore (engine.py):**
```python
async with self._semaphore.slot(worker_id, timeout_seconds=self._semaphore_timeout):
    # Slot is held here. If this block raises, slot is released in finally.
    semaphore_slots_held.labels(device_id=device_id).inc()
    heartbeat_task = asyncio.create_task(
        self._run_heartbeat(req_repo, request_id, worker_id)
    )
    try:
        await self._run_workflow(...)
    finally:
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass
        semaphore_slots_held.labels(device_id=device_id).dec()
```

**How the TTL self-healing works:**
1. Worker acquires slot → Redis Hash field `sem:{device_id}` gets field `worker_id = timestamp`, TTL = `slot_ttl` seconds
2. Heartbeat background task calls `semaphore.renew(worker_id)` every P-5 seconds → resets TTL to `slot_ttl`
3. If worker dies → heartbeat stops → TTL ticks down → slot expires automatically
4. Other workers can now acquire that slot on their next poll

**The critical invariant:** The heartbeat renews **both** `requests.last_heartbeat_at` in the DB **and** the semaphore slot TTL. The reclaim sweeper uses `last_heartbeat_at` to detect dead workers. The semaphore uses the TTL for the same purpose. They must stay in sync.

```python
# From engine.py _run_heartbeat():
async def _run_heartbeat(self, req_repo, request_id, worker_id):
    while True:
        await asyncio.sleep(self._heartbeat_interval)
        try:
            req_repo.update_heartbeat(request_id)                      # DB
            await self._semaphore.renew(worker_id, int(self._heartbeat_interval * 3))  # Redis
        except Exception as exc:
            log.warning("workflow.heartbeat_error", error=str(exc))
```

---

### Layer 4 — Token bucket (per-device rate limiter)

**What it does:** Limits the rate of HTTP calls to a specific F5 device. The semaphore limits *concurrent* workflows; the token bucket limits the *rate* of actual HTTP requests regardless of concurrency.

**Why both?** If P-1 = 8 concurrent workflows and each workflow makes 10 F5 calls, that's 80 concurrent HTTP calls potentially. The token bucket smooths this to a rate the F5 control plane can sustain without saturating mcpd.

**Algorithm:** Each "token" represents permission to make one HTTP call. Tokens refill at rate P-3 per second. Maximum bucket size is P-2 (burst ceiling). If the bucket is empty, the call is rejected immediately (not queued — that's Layer 3's job).

**Where it lives:**
- Implementation: `app/coordination/ratelimit.py`
- Lua script: `app/coordination/scripts/token_bucket.lua`
- Used in: `app/clients/f5/gtm.py::_consume_token()` — called before every HTTP call

**Code — consume before every F5 HTTP call (gtm.py):**
```python
async def _consume_token(self) -> None:
    if self._token_bucket is not None:
        allowed = await self._token_bucket.consume(1)
        if not allowed:
            raise F5Error("Rate limit bucket exhausted — request rejected before F5 call")
```

**The Lua script does this atomically (token_bucket.lua sketch):**
```lua
-- args: capacity, refill_rate, tokens_requested, now
local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'last_refill')
local tokens = tonumber(bucket[1]) or capacity
local last_refill = tonumber(bucket[2]) or now

-- refill based on elapsed time
local elapsed = now - last_refill
local new_tokens = math.min(capacity, tokens + elapsed * refill_rate)

if new_tokens >= tokens_requested then
    -- consume and update
    redis.call('HMSET', KEYS[1], 'tokens', new_tokens - tokens_requested, 'last_refill', now)
    return 1  -- allowed
else
    redis.call('HMSET', KEYS[1], 'tokens', new_tokens, 'last_refill', now)
    return 0  -- rejected
end
```

**Why Lua for all 4 layers?** Any of these operations that involves a read-then-write is a race condition if done in Python. For example: "check if tokens > 0, then decrement" — two pods doing this simultaneously can both see tokens > 0 and both decrement, exceeding the limit. Lua scripts run atomically inside Redis — no other command executes between the read and the write.

---

### How all 4 layers connect in one request lifecycle

```
POST /wideip arrives
        │
[app/api/routes.py]
        ├─ Layer 1: read queue_depth:global and queue_depth:{device}  [admission.py]
        ├─ Layer 2: peek circuit breaker state for device              [admission.py]
        │
        ├─ Atomic DB insert (unique index guard)
        ├─ RECEIVED → QUEUED
        ├─ run_gtm_workflow.delay(request_id, device_id)
        ├─ Layer 1: INCR queue_depth:global, queue_depth:{device}     [routes.py]
        └─ return 202

[Celery worker picks up task]
        │
[app/tasks/workflows.py → app/workflow/engine.py]
        │
        ├─ Atomic DB claim: UPDATE WHERE status='QUEUED' AND request_id=?
        │   (if 0 rows affected → another worker owns it → abort)
        │
        ├─ Layer 3: semaphore.slot(worker_id, timeout=P4)             [engine.py]
        │   (if timeout → revert to QUEUED, re-enqueue)
        │   (if acquired → hold slot until workflow completes)
        │
        ├─ Start heartbeat task (renews DB + semaphore TTL every P-5s)
        │
        ├─ For each step (monitor → pool → members → wideip → cname):
        │   ├─ Layer 4: token_bucket.consume(1)                       [gtm.py]
        │   │   (if empty → raise F5Error → step fails → rollback)
        │   ├─ HTTP call to F5 or Infoblox
        │   └─ Layer 2: record_success/failure/timeout on circuit breaker [gtm.py]
        │
        ├─ COMPLETED
        ├─ Layer 1: DECR queue_depth:global, queue_depth:{device}     [engine.py]
        └─ Layer 3: semaphore slot released in finally                 [engine.py]
```

---

### Where to find each layer in the codebase

| Layer | Core logic | Wiring point |
|---|---|---|
| Queue depth (L1) | `app/api/admission.py` (read) | `app/api/routes.py` (incr), `app/workflow/engine.py::_decrement_queue_depth()` (decr) |
| Circuit breaker (L2) | `app/coordination/breaker.py` + `scripts/breaker_record.lua` | `app/api/admission.py` (read state), `app/clients/f5/gtm.py::_record_outcome()` (write outcomes) |
| Semaphore (L3) | `app/coordination/semaphore.py` + `scripts/semaphore_*.lua` | `app/workflow/engine.py::execute()` (acquire/release), `engine.py::_run_heartbeat()` (renew) |
| Token bucket (L4) | `app/coordination/ratelimit.py` + `scripts/token_bucket.lua` | `app/clients/f5/gtm.py::_consume_token()` (before every HTTP call) |

---

## Key questions still open (do not assume answers)

1. **TACACS+ source name on BIG-IP** — run `tmsh list auth tacacs` and set `F5_LOGIN_PROVIDER_NAME` to the object name shown.
2. **Existing app DDL** — ask for the actual table DDL before touching the DB schema. The scaffold DDL in `001_initial.sql` may conflict with existing tables.
3. **P-n values** — none can be set until WP-0 measurements are done. Do not substitute any number.
4. **Consumer HTTP timeout** — if the consumer's timeout is shorter than the ~150s workflow, they will retry mid-flight, causing a retry storm. Confirm with the consumer team.
5. **Delete cap (P-9)** — how many WideIP deletes per hour is acceptable? Business decision, not a technical one.

---

## Working style

- Direct and concise. Reasoning before implementation.
- State uncertainty plainly. "I don't know" is better than a confident guess.
- Prefer the simplest change that satisfies the requirement.
- Behind feature flags always — never change live behaviour in one step.
- Ask rather than assume about existing table shapes, function signatures, or behaviour.
