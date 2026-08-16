# F5 GTM Automation — Implementation Plan

**Audience:** implementing agent (Claude Opus 4.6) working with the system owner.
**Format:** work packages (WP) → tasks (T). Each task has a spec and acceptance criteria. Build in WP order; tasks within a WP may parallelise unless a dependency is stated.

---

## Rules for the implementing agent

1. **Never invent a value for a `P-n` parameter.** All load-governing numbers come from WP-0 measurements. If WP-0 output is missing, stop and ask.
2. **Never assume an F5 or Infoblox API shape.** Confirm every endpoint, field name, and response format against official documentation for the *installed version*, or against a live dev call. Field names differ across versions.
3. The existing codebase cannot be shared. Ask for interface shapes (table DDL, function signatures, config structure) before writing anything that integrates with existing code.
4. A task is not done until its acceptance criteria pass. Do not mark partial completion as done.

---

## 1. Locked decisions

| # | Decision | Value | Rationale |
|---|---|---|---|
| D-1 | Redis topology | **One shared primary; both DCs connect; replica in second DC for manual failover** | Per-device semaphore, rate limiting and breaker state require cross-pod state. Per-pod Redis makes them impossible. Cross-DC latency is ~6–8 Lua round trips per workflow — negligible against a 150s workflow. |
| D-2 | Redis role | Dispatch only. MSSQL remains sole source of truth. | Already the case; preserve it. Enqueue carries `request_id` only, never payload. |
| D-3 | Redis eviction policy | `noeviction`, mandatory | Any `allkeys-*` policy silently deletes queued tasks. Persistence does not prevent this. |
| D-4 | Redis unavailable | **Fail closed** — reject new work with 503 + `Retry-After` | Failing open means unlimited concurrency at F5. |
| D-5 | Concurrency scoping | **Per target device**, not global | 4 prod devices are separate grids = independent capacity pools. |
| D-6 | Rate limit algorithm | Token bucket per device (Redis Lua) | Permits controlled burst then settles. Leaky bucket only if WP-0 shows the device degrades on any burst. |
| D-7 | Concurrency floor | Must not drop below what drains arrivals | At 8 concurrent, throughput ~190/hr vs ~555/hr arriving. Queue never drains. Protect F5 with per-device scoping + breaker, not a lower global cap. |
| D-8 | Same FQDN, different payload, in flight | Reject `409` with running request details | Owner decision, confirmed. |
| D-9 | Same FQDN, same idempotency key | Return `200` with original `request_id` (in flight or completed) | True idempotent replay. Not an error. |
| D-10 | Reconciler auto-delete | **Off. Report-only at launch.** | Inherited drift from the Ansible era. An auto-deleting reconciler on first prod run is the most destructive possible failure. |
| D-11 | App/worker split | App pods run FastAPI + workers; Redis runs as its own pod with generous memory | Removes Redis from app pod memory contention (the current crash cause). |

---

## 2. Target runtime architecture

```
                    OpenShift Route (round-robin)
                              │
              ┌───────────────┴───────────────┐
              │                               │
         DC-A pods                       DC-B pods
    ┌─────────────────┐             ┌─────────────────┐
    │ FastAPI + Celery│             │ FastAPI + Celery│
    │    workers      │             │    workers      │
    └────────┬────────┘             └────────┬────────┘
             │                               │
             └───────────┬───────────────────┘
                         │
        ┌────────────────┼────────────────┐
        │                │                │
   ┌────▼─────┐    ┌─────▼──────┐   ┌─────▼──────┐
   │  Redis   │    │   MSSQL    │   │  F5 grids  │
   │ (shared) │    │ (truth)    │   │  ×4 indep. │
   │          │    │            │   │  Infoblox  │
   │ • broker │    │ • requests │   └────────────┘
   │ • semaphore    │ • steps   │
   │ • buckets│    │ • objects  │
   │ • breaker│    │ • audit    │
   └──────────┘    └────────────┘
        │
   ┌────▼─────┐
   │  Redis   │
   │ replica  │  (DC-B, manual failover)
   └──────────┘
```

Celery beat: single instance (as today), auto-restarting.

### 2.1 Redis configuration

```
maxmemory              <60–70% of container memory limit>
maxmemory-policy       noeviction
appendonly             yes
appendfsync            everysec
```

**Why `maxmemory` must be set well below the container limit.** Default `maxmemory 0` means unlimited — Redis grows until the OpenShift OOM killer terminates the pod. That is the most likely explanation for the current crashes. Additionally, both RDB snapshots and AOF rewrites **fork the process**, and copy-on-write can transiently increase memory well above steady state. Setting `maxmemory` equal to the container limit guarantees an eventual OOM kill during a fork. Leave 30–40% headroom.

**Why `noeviction` specifically.** Any `allkeys-*` policy deletes keys to make room, and Redis does not distinguish a Celery task payload from a cache entry. Tasks disappear with no error raised anywhere. Persistence does not protect against this — evictions are persisted.

**Consequence of `noeviction` that must be handled in code.** When memory is exhausted, writes fail with an OOM error rather than silently succeeding. `.delay()` will raise. This is the correct behaviour, but the API must catch it and return `503` + `Retry-After` (D-4), not a `500`. Without this handler, memory pressure turns into user-visible server errors instead of clean backpressure.

**Two changes that cut memory footprint substantially:**

1. **Enqueue `request_id` only, never the payload** (D-2). A queue of 5,000 request IDs is a few MB. A queue of 5,000 full GTM payloads is orders of magnitude larger. If the current code passes payloads to `.delay()`, this is the primary memory driver and fixing it may resolve the crashes on its own.

2. **Disable or shorten the result backend.** Status already lives in MSSQL, so Celery results are redundant. Set `task_ignore_result = True`, or if results are needed for some paths, set `result_expires` to a low value. Default retention accumulates result keys for a day.

**Monitoring:** alert on `used_memory / maxmemory` above 70%, and on `rdb_last_bgsave_status` / `aof_last_bgrewrite_status` failures — a failing background save is an early warning of fork-time memory pressure.

---

## 3. Request flow (design)

### 3.1 API path — synchronous, must complete in milliseconds

```
1.  Receive request
2.  Validate payload schema                        → 400 on failure
3.  Resolve target_device from payload             → 400 if unresolvable
4.  Compute idempotency_key
        = sha256(action | wip_fqdn | normalise(payload))
        normalise = sorted keys, lowercased FQDNs, stripped whitespace
5.  ADMISSION CHECKS (in order, cheapest first):
      a. Redis reachable?                          → 503 + Retry-After  [D-4]
      b. Kill switch engaged?                      → 503 + Retry-After
      c. Global queue depth < P-7?                 → 503 + Retry-After
      d. Target device breaker closed OR
         device queue depth < P-8?                 → 503 + Retry-After
6.  CLAIM in MSSQL — single atomic INSERT guarded by
    unique filtered index on active wip_fqdn
      ├─ success        → row written, status = RECEIVED
      └─ duplicate key  → SELECT existing row, then:
                            same idempotency_key  → 200 + original request_id  [D-9]
                            different key         → 409 + running details      [D-8]
7.  Transition RECEIVED → QUEUED
8.  Enqueue Celery task with request_id ONLY       [D-2]
9.  Return 202 { request_id, status, status_url, Retry-After }
```

**The API never calls F5 or Infoblox.** All external work happens in workers.

### 3.2 Worker path

```
1.  Load request row by request_id
2.  ATOMIC CLAIM:
      UPDATE requests
      SET status='RUNNING', worker_id=?, pod_id=?,
          started_at=NOW(), last_heartbeat_at=NOW()
      WHERE request_id=? AND status='QUEUED'
    → 0 rows affected = another worker owns it → abort silently
3.  ACQUIRE per-device semaphore slot (timeout P-4)
      └─ timeout → revert to QUEUED, re-enqueue with backoff, exit
4.  START heartbeat renewer (background, interval P-5)
      renews: requests.last_heartbeat_at AND semaphore slot TTL
5.  PRE-VALIDATION PHASE
      → any failure = REJECTED or FAILED, no external writes made
6.  IMPLEMENTATION PHASE — ordered steps (§3.3)
      → failure triggers ROLLING_BACK
7.  POST-VALIDATION PHASE
      → mismatch = VERIFY_FAILED → remediation queue
8.  Transition → COMPLETED
9.  FINALLY: stop heartbeat, release semaphore slot
```

Step 3 ordering matters: claim the DB row **before** acquiring the slot, so a slot is never held by a worker that has no work.

### 3.3 Step execution pattern (every step, without exception)

```
1. Write request_steps row with intent_json, status=PENDING
2. READ current state of the object from target system
3. Persist pre_state_json          ← REQUIRED for correct rollback
4. COMPARE desired vs actual
5. ACT:
     absent    → create
     differs   → modify
     identical → NO-OP, mark SUCCEEDED    ← this branch is what makes it idempotent
6. Write result_json, status=SUCCEEDED
7. READ BACK and verify
```

Step 5's no-op branch is the most commonly omitted and its absence is what turns every retry into an error.

### 3.4 Object ordering

```
CREATE:  monitor → pool → pool members → WideIP → CNAME
DELETE:  CNAME → WideIP → pool members → pool → monitor
```

**Confirm the exact dependency graph against F5 docs for the installed version and validate in dev — do not assume.**

The CNAME must be removed *before* the WideIP on delete, or DNS resolves to something that no longer exists.

If T-0.5 confirms iControl REST transaction support for GTM objects, the four F5 steps collapse into one atomic transaction and most partial states cease to exist. Prefer that path.

### 3.5 Rollback

Compensating steps, reverse order, driven by `request_steps` rows — never by assumption about what "probably" happened.

```
FOR each completed step, in reverse order:
    IF pre_state_json shows object did NOT exist  → delete it
    IF pre_state_json shows object DID exist      → restore prior state
                                                    (NEVER delete)
    Each compensation is itself idempotent and retryable
ON rollback failure → ROLLBACK_FAILED → NEEDS_ATTENTION + notify
                      (never silently retry in a loop)
```

The pre-existing-object rule is critical: a failed PUT must not delete an object that existed before the request arrived.

### 3.6 State machine

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

`NEEDS_ATTENTION` is terminal. Nothing automatic exits it. Entry raises a notification.

---

## 4. Module structure

```
app/
  api/
    routes.py           POST/PUT/DELETE + status endpoint
    schemas.py          pydantic request/response models
    admission.py        §3.1 step 5 checks
    idempotency.py      key computation + normalisation
  core/
    config.py           all P-n parameters, runtime-reloadable
    metrics.py          §8 metric definitions
    logging.py          structured logging, request_id propagation
  domain/
    states.py           state machine + transition guards
    models.py           domain objects
  db/
    migrations/         DDL
    repositories.py     requests, steps, managed_objects, audit
    claim.py            atomic claim + reclaim queries
  coordination/         ALL Redis primitives, ALL atomic via Lua
    semaphore.py        per-device slots with TTL
    ratelimit.py        per-device token bucket
    breaker.py          per-device circuit breaker
    scripts/            .lua files
  clients/
    f5/
      session.py        connection pool, keep-alive
      auth.py           token auth + cached token per device
      gtm.py            wideip / pool / member / monitor operations
    infoblox/
      session.py        WAPI cookie reuse
      records.py        CNAME operations
  workflow/
    engine.py           §3.2 orchestration
    steps/              one module per object type
    compensations/      one module per object type
  tasks/
    celery_app.py
    workflows.py
    beat.py
  recovery/
    reclaim.py          stale RUNNING/QUEUED reclamation
    remediation.py      failed-step retry queue
    reconciler.py       drift sweep
  ops/
    controls.py         kill switch, dry-run, destructive caps
    status.py           operational status endpoint
```

---

## 5. Work packages

### WP-0 — Measure (BLOCKING; nothing sized without it)

| Task | Goal | Spec | Acceptance |
|---|---|---|---|
| **T-0.1** | Where the 150s goes | Instrument one POST at ~50 concurrent. Break down: F5 writes (per object type), F5 reads (pre vs post separately), Infoblox reads/writes, MSSQL, app logic, explicit sleeps/polls/waits | Timing table with percentages. Highest-value task in the plan. |
| **T-0.2** | API call budget | Count actual outbound calls for one POST, PUT, DELETE. Split: F5 pre-read / F5 write / F5 post-read / Infoblox read / Infoblox write / auth | Call-count matrix per action |
| **T-0.3** | F5 auth mode | Ask F5 team or check device: do admin accounts authenticate **locally** or against **LDAP/AD/TACACS+/RADIUS**? | Documented answer. If remote → T-2.2 becomes critical priority, not optimisation |
| **T-0.4** | BIG-IP version + AS3 | `tmsh show sys version` **from inside the BIG-IP tenant** (not F5OS/hardware version). Then: is AS3 installed, which version? | Version string + AS3 presence. If AS3 present, check GSLB class support **against the schema docs for that exact version** |
| **T-0.5** | Transaction support | Determine whether GTM objects (WideIP/pool/member/monitor) are supported inside `/mgmt/tm/transaction`. Verify against F5 docs for the installed version AND prove with a dev test | Supported/not + test evidence. If supported → redesign F5 steps as one atomic transaction |
| **T-0.6** | Load curve | From the existing 30-min/50-user Locust run extract: total completed, req/sec, p50/p95/p99, error count/types, peak concurrent in-flight. Then step concurrency upward measuring the same | Throughput/latency curve; identified knee |
| **T-0.7** | Device-side capacity | With F5 team, observe during load: control-plane CPU, mcpd behaviour, config-save duration, error logs. Config writes serialise through mcpd, so the real limit is likely **config transactions per device**, not RPS | Which metric saturates first, and at what client concurrency |
| **T-0.8** | Session reuse audit | For F5 and Infoblox: is a single Session reused with keep-alive? Is the WAPI cookie genuinely reused? Is TLS renegotiated per call? | Documented current behaviour per integration |
| **T-0.9** | Redis policy | `CONFIG GET maxmemory-policy` and `maxmemory` on every instance | Confirmed `noeviction`. **Remediate immediately if not** — do this today, ahead of everything else |
| **T-0.10** | Drift baseline | Read-only comparison MSSQL ↔ F5 ↔ Infoblox across managed WideIPs. Categorise every discrepancy | Drift inventory by category and count. Becomes WP-6 requirements + regression baseline |

### WP-1 — Data foundation

| Task | Goal | Spec | Acceptance |
|---|---|---|---|
| **T-1.1** | Schema | Tables: `requests`, `request_steps`, `managed_objects`, `state_transitions`, `remediation_queue`. Fields per §6. **Ask for existing DDL first** and align conventions | Migrations apply and roll back cleanly on a copy of the real schema |
| **T-1.2** | Concurrency guard | `CREATE UNIQUE INDEX UX_requests_active_wip ON requests(wip_fqdn) WHERE status IN ('RECEIVED','VALIDATING','QUEUED','RUNNING','VERIFYING');` plus duplicate-key handling that returns the existing request | Test: 50 concurrent identical POSTs → exactly 1 row created, 49 receive existing request_id |
| **T-1.3** | State machine | Transition table + guard function. Invalid transitions raise. Every transition writes to `state_transitions` with actor and reason | Unit tests cover every valid and invalid transition |
| **T-1.4** | Idempotency keys | Deterministic normalisation then sha256 | Payloads differing only in key order / casing / whitespace produce identical keys |
| **T-1.5** | Heartbeating | Workers update `last_heartbeat_at` every P-5 seconds via background renewer | Heartbeat continues through a long-running step; stops on completion |

### WP-2 — Client layer (cheapest wins; may run parallel with WP-1)

| Task | Goal | Spec | Acceptance |
|---|---|---|---|
| **T-2.1** | Session reuse | One pooled Session per target system with keep-alive; pool sized to concurrency. Fix anything T-0.8 found | Verified single TLS handshake across N sequential calls |
| **T-2.2** | F5 token auth | Replace per-call basic auth. Obtain token, cache in Redis per device, refresh before expiry (not on 401), guard refresh with a lock to prevent stampede. **Verify endpoint, default lifetime and extension mechanism against F5 docs for the T-0.4 version** | 1,000 calls consume 1 token, not 1,000. Concurrent refresh produces one token |
| **T-2.3** | Retry policy | Exponential backoff + jitter on retryable errors. **Timeouts are NOT retryable blind** — read back to determine actual state first, then converge | Injected timeout after a successful create does not produce a duplicate-create error |
| **T-2.4** | Infoblox client | WAPI cookie reuse; grid-master write target confirmed | Single authentication across a batch of calls |
| **T-2.5** | Re-measure | Repeat T-0.1 after WP-2 | New service time recorded. **Optimal concurrency may have changed — resize P-1** |

### WP-3 — Shared Redis + coordination

| Task | Goal | Spec | Acceptance |
|---|---|---|---|
| **T-3.1** | Deploy shared Redis | Per D-1 and D-11. `noeviction`, memory headroom alerting, replica in DC-B, documented manual failover procedure and accepted RTO | Both DCs connect; failover procedure rehearsed and timed |
| **T-3.2** | Semaphore | Per-device slots. Acquire/release **atomic via Lua**. TTL per slot so a dead worker's slot is reclaimed. Renewed by heartbeat | Kill a worker holding a slot → slot reclaimed within TTL. Never exceeds P-1 for any device |
| **T-3.3** | Token bucket | Per-device, Lua. Params P-2/P-3 from WP-0. Separate bucket for Infoblox grid master | Sustained rate matches configured refill; burst capped at bucket size |
| **T-3.4** | Circuit breaker | Per device: closed → open → half-open. Trip on error rate / p95 latency / consecutive timeouts. **When open, requests stay QUEUED — they do not fail.** Half-open probes with increasing backoff. State in Redis, visible to all pods | One device failing does not affect the other three. Breaker state identical across pods within one poll interval |
| **T-3.5** | Admission control | §3.1 step 5. Fail closed when Redis is down | Redis stopped → new requests get 503 + Retry-After; in-flight work continues; sweeper recovers |
| **T-3.6** | Feature flags | Every WP-3 component independently enable/disable at runtime without redeploy | Each toggles live and the effect is observable in metrics |

### WP-4 — Workflow engine

| Task | Goal | Spec | Acceptance |
|---|---|---|---|
| **T-4.1** | Engine | §3.2 orchestration including atomic claim, slot acquire/release ordering, heartbeat lifecycle, guaranteed release in `finally` | Slot always released even on unhandled exception |
| **T-4.2** | Steps | One module per object type, all following §3.3 exactly, including the no-op branch and `pre_state_json` capture | **Every step run twice leaves identical state and raises no error** |
| **T-4.3** | Ordering | §3.4, confirmed against docs + dev validation | Delete removes CNAME before WideIP |
| **T-4.4** | Compensations | §3.5. Pre-existing objects restored, never deleted | Test: PUT against a pre-existing WideIP fails mid-way → WideIP still exists with original config |
| **T-4.5** | Post-validation | Read back every object, compare to intent, mismatch → VERIFY_FAILED → remediation | Seeded mismatch is detected, not passed |
| **T-4.6** | Transaction path | **Only if T-0.5 confirms support** — replace the 4 F5 steps with one atomic transaction | All-or-nothing on F5; partial F5 state becomes impossible |

### WP-5 — Recovery

| Task | Goal | Spec | Acceptance |
|---|---|---|---|
| **T-5.1** | Reclaim (extends existing sweeper) | Distinguish two cases: **QUEUED never claimed** → safe to re-enqueue immediately. **RUNNING with a live heartbeat** → NOT safe; a slow worker is still working. Only reclaim RUNNING when `last_heartbeat_at` is older than P-6 (suggest 3× heartbeat interval). Reclaim must be an atomic conditional update on current worker_id | Slow worker (long step, heartbeat healthy) is never reclaimed. Killed worker reclaimed within P-6. No double execution under either condition |
| **T-5.2** | Remediation queue | Failed steps with a known recovery. Exponential backoff + jitter, capped attempts, then NEEDS_ATTENTION. Covers "WideIP created, CNAME failed after retries" | Failed CNAME retried on schedule; escalates after cap with full diagnostic |
| **T-5.3** | Notifications | Alert on every NEEDS_ATTENTION entry with request_id, failure category, diagnostic | Alert fires end-to-end in dev |
| **T-5.4** | Failure matrix | Implement handling for every row in §7 | Each scenario has a test with injected fault |

### WP-6 — Reconciler (report-only first)

| Task | Goal | Spec | Acceptance |
|---|---|---|---|
| **T-6.1** | Drift detection | Compare MSSQL ↔ F5 ↔ Infoblox. Paginated (never enumerate 10k at once), rate-limited through the same token bucket at lower priority, checkpointed/resumable, scheduled off-peak | Full sweep completes without measurable impact on request latency |
| **T-6.2** | Categorisation | Categories per §9, each with a defined default action | Seeded drift of every category detected and correctly categorised |
| **T-6.3** | Report mode | Detect and report only. **Zero write capability at launch** [D-10] | Runs against prod producing reports and taking no action |
| **T-6.4** | Incremental mode | Prioritise objects with oldest `last_verified_at` | Each run covers the stalest portion rather than everything |
| **T-6.5** | Selective convergence | Per-category enable flags, default all off. Auto-delete last or never | Enabling one category does not enable others |

### WP-7 — Operational controls

| Task | Goal | Spec | Acceptance |
|---|---|---|---|
| **T-7.1** | Kill switch | One runtime flag halts all F5/Infoblox writes without redeploy. Requests queue; nothing lost | Toggles in under 5 seconds; in-flight work completes or parks cleanly |
| **T-7.2** | Dry-run | Compute full workflow, log every intended call with payload, execute nothing | Full POST in dry-run produces a complete call log and zero external changes |
| **T-7.3** | Destructive cap | Refuse more than P-9 deletes per rolling window without explicit override | Cap enforced; override path audited |
| **T-7.4** | Per-device disable | Remove one device from rotation for maintenance | Other three unaffected |
| **T-7.5** | Status endpoint | Per-device breaker state, queue depths, slot utilisation, kill-switch/dry-run state, remediation depth | On-call can assess the system in one request |

### WP-8 — Validation and rollout

| Task | Goal | Acceptance |
|---|---|---|
| **T-8.1** | Concurrency tests | 50 simultaneous identical POSTs → 1 processes, 49 get running response. Worker killed mid-workflow → reclaimed, no double execution. Slow worker never reclaimed prematurely |
| **T-8.2** | Retry-storm replay | Burst of duplicates across a small FQDN set → absorbed at admission, consumes no worker capacity |
| **T-8.3** | Load campaign | Step concurrency in stages measuring throughput, p95/p99, errors, and device-side indicators (T-0.7). Sustain the safe level for a full 9-hour-equivalent window |
| **T-8.4** | Chaos | Kill Redis (fail closed verified); kill a pod mid-workflow; one F5 unreachable (only that queue affected); Infoblox unreachable; DB failover |
| **T-8.5** | Rollback correctness | Every scenario in §7 with injected faults; pre-existing objects never destroyed |

---

## 6. Table field reference

**`requests`** — `request_id`, `idempotency_key`, `action`, `wip_fqdn`, `target_device`, `payload_hash`, `payload_json`, `status`, `created_at`, `updated_at`, `started_at`, `completed_at`, `worker_id`, `pod_id`, `last_heartbeat_at`, `attempt_count`, `last_error`, `needs_attention_reason`

**`request_steps`** — `step_id`, `request_id`, `step_name`, `step_order`, `target_system`, `object_type`, `object_key`, `intent_json`, `pre_state_json`, `result_json`, `status`, `attempts`, `error`, `started_at`, `completed_at`, `compensation_status`

**`managed_objects`** — `object_id`, `wip_fqdn`, `object_type`, `object_key`, `target_device`, `desired_state_json`, `last_verified_at`, `drift_detected_at`, `drift_details_json`, `owning_request_id`, `status`

**`state_transitions`** (append-only) — `request_id`, `from_status`, `to_status`, `reason`, `actor`, `timestamp`

**`remediation_queue`** — `remediation_id`, `request_id`, `step_id`, `failure_category`, `retry_count`, `next_retry_at`, `escalated_at`, `resolution`

---

## 7. Failure matrix

| # | Scenario | Detection | Action |
|---|---|---|---|
| 1 | Monitor created, pool fails | Step status | Rollback monitor (only if not pre-existing) |
| 2 | Pool created, WideIP fails | Step status | Rollback pool + monitor |
| 3 | WideIP created, CNAME fails | Step status | Retry with backoff → remediation queue → NEEDS_ATTENTION |
| 4 | Post-validation mismatch | Read-back compare | Attempt reconcile; escalate on failure |
| 5 | Worker dies mid-workflow | Stale heartbeat | Reclaim per T-5.1 safety condition |
| 6 | F5 returns 5xx | HTTP status | Retry with backoff; count toward breaker |
| 7 | F5 timeout, outcome unknown | Timeout | **Read back first** — never blind-retry. Outcome is unknown, not failed |
| 8 | Infoblox unavailable | Connection error | Queue CNAME for retry; F5 state stands |
| 9 | DB unavailable mid-workflow | Connection error | Fail request; sweeper recovers from last durable state |
| 10 | Redis unavailable | Connection error | Fail closed at admission; in-flight continues |
| 11 | DELETE, object already absent | Pre-validation | Succeed idempotently; log; do not error |
| 12 | Concurrent same-FQDN | Unique index | Reject with running details |
| 13 | Rollback fails | Compensation status | NEEDS_ATTENTION + notify; never loop |
| 14 | Legacy Ansible orphan | Reconciler | Categorise; **never auto-delete** |
| 15 | Device unreachable | Breaker open | Hold that device's queue; alert |

---

## 8. Drift categories

| Category | Default action |
|---|---|
| In DB, missing in F5 | Flag; auto-create only if explicitly enabled |
| In F5, not in DB | **Flag only — never auto-delete** |
| Both present, attributes differ | Flag with diff; converge only if enabled |
| WideIP present, CNAME missing | Queue CNAME creation (remediation) |
| CNAME present, WideIP missing | High-severity alert — DNS points at nothing |
| Marked `pending_delete`, still present | Retry delete behind confirmation gate |

---

## 9. Parameters — all sourced from WP-0

| ID | Parameter | Source | Notes |
|---|---|---|---|
| P-1 | Per-device concurrency | T-0.6, T-0.7 | Must satisfy D-7. Global = sum across devices |
| P-2 | Token bucket size (per device) | T-0.7 | |
| P-3 | Token refill rate (per device) | T-0.7 | |
| P-4 | Semaphore acquire timeout | Derived from P-1 and service time | |
| P-5 | Heartbeat interval | Choose; suggest 15–30s | |
| P-6 | Stale heartbeat threshold | 3 × P-5 | |
| P-7 | Global queue depth limit | Derived from throughput and acceptable wait | |
| P-8 | Per-device queue depth limit | Derived from P-1 | |
| P-9 | Max deletes per window | Business decision | |
| P-10 | Breaker thresholds | T-0.6, T-0.7 | error rate, p95 latency, consecutive timeouts |

**Every P-n must be runtime-adjustable without redeploy.**

---

## 10. Observability

**Metrics (per device where applicable):** request rate by action/status; workflow duration histogram; step duration by type; F5 and Infoblox call latency and error rate; semaphore slots held and wait time; bucket rejections; breaker state; queue depth; reclaim count; remediation depth; NEEDS_ATTENTION count.

**Logs:** `request_id` on every line; every external call logs system, device, object key, duration, outcome; every state transition logs from → to → reason → actor.

**Alerts:** any NEEDS_ATTENTION; breaker opens; queue depth above threshold; reclaim rate above normal; drift above baseline; remediation queue not draining; Redis memory headroom low.

---

## 11. Rollout gates

| Gate | Requires |
|---|---|
| Dev → UAT | WP-0 through WP-5 complete; WP-7 controls live; T-8.1, T-8.2, T-8.5 pass |
| UAT → Prod (read paths) | T-8.3 sustained load pass on UAT's 2 devices; T-8.4 chaos pass |
| Prod write enable | Kill switch and dry-run verified in prod; per-device enable, one device first |
| Reconciler enable | Report-only for a defined observation period; T-0.10 baseline understood; output trusted by the owner |
| Reconciler convergence | Per category, individually, deliberately. Auto-delete last or never |

---

## 12. Open decisions for the owner

1. Redis HA approach given no Sentinel today; accepted RTO for manual failover
2. Blast-radius controls (WP-7) — in or out of scope
3. Which drift categories, if any, ever get auto-convergence
4. Whether to pursue AS3 declarative config if T-0.4 confirms GSLB support
5. Whether to pursue transactions if T-0.5 confirms GTM support
6. Consumer retry policy conversation — currently deferred. **If their HTTP client timeout is shorter than the 3-minute workflow, they abandon and retry mid-flight, and that is a direct cause of the retry storm that no backend change can fix.** Worth confirming their timeout value even if the policy conversation stays deferred
7. ServiceNow CR path, currently disabled for latency — track as a separate compliance workstream

---

## 13. Known unknowns

Recorded so they are not filled with assumptions: BIG-IP tenant version and AS3 availability (T-0.4); GTM transaction support (T-0.5); real per-request API call counts (T-0.2); where the 150s goes (T-0.1); F5 auth mode (T-0.3); measured per-device write capacity (T-0.6, T-0.7); existing schema DDL and code structure (not shareable — ask); drift volume (T-0.10); consumer HTTP timeout and retry interval.
