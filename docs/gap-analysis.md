# Gap Analysis — F5 GTM Automation Service

Generated: 2026-08-16. This is a greenfield build — no prior codebase existed.

---

## 1. Task-by-task status

### WP-0 — Measure (all BLOCKED on live system access)

| Task | Status | Notes |
|---|---|---|
| T-0.1 | BLOCKED | Requires live F5 timing at ~50 concurrent. Cannot instrument without a running system. |
| T-0.2 | BLOCKED | Partially answered from research: 1 auth call + 4 F5 writes (monitor/pool/members/WideIP) + 1 Infoblox write per POST. Pre/post read counts unconfirmed. |
| T-0.3 | BLOCKED | F5 auth mode (local vs LDAP/AD/TACACS+) must be confirmed with F5 team. |
| T-0.4 | BLOCKED | BIG-IP tenant version and AS3 availability must be confirmed with `tmsh show sys version`. |
| T-0.5 | **EXISTS (research complete)** | GTM objects confirmed NOT supported in iControl REST transactions. T-4.6 is CANCELLED. |
| T-0.6 | BLOCKED | Load curve requires live Locust run. |
| T-0.7 | BLOCKED | Device-side capacity requires F5 team observation during load. |
| T-0.8 | BLOCKED | Session reuse audit requires live dev calls. Code is written for keep-alive (httpx AsyncClient) but unverified. |
| T-0.9 | BLOCKED | `CONFIG GET maxmemory-policy` must be run against every Redis instance. **Do this today.** |
| T-0.10 | BLOCKED | Drift baseline requires read-only sweep of prod MSSQL ↔ F5 ↔ Infoblox. |

### WP-1 — Data foundation

| Task | Status | File(s) |
|---|---|---|
| T-1.1 | EXISTS | `app/db/migrations/001_initial.sql`, `app/domain/models.py` |
| T-1.2 | EXISTS | `app/db/migrations/001_initial.sql` (UX_requests_active_wip partial index), `app/db/claim.py` |
| T-1.3 | EXISTS | `app/domain/states.py` (VALID_TRANSITIONS, InvalidTransitionError, transition()) |
| T-1.4 | EXISTS | `app/api/idempotency.py` |
| T-1.5 | EXISTS | `app/workflow/engine.py` (_run_heartbeat) |

### WP-2 — Client layer

| Task | Status | File(s) |
|---|---|---|
| T-2.1 | EXISTS | `app/clients/f5/session.py`, `app/clients/infoblox/session.py` — httpx keep-alive; unverified against live dev |
| T-2.2 | EXISTS | `app/clients/f5/auth.py` — token cached in Redis per device, stampede guard via NX lock |
| T-2.3 | EXISTS | `app/clients/f5/gtm.py`, `app/clients/infoblox/records.py` — timeout raises specific error, never blind-retried |
| T-2.4 | EXISTS | `app/clients/infoblox/session.py` — ibapauth cookie reuse via httpx cookie jar; grid-master targeting via INFOBLOX_HOST setting |
| T-2.5 | BLOCKED | Re-measure requires live system |

### WP-3 — Shared Redis + coordination

| Task | Status | File(s) |
|---|---|---|
| T-3.1 | BLOCKED | Redis deployment is an infrastructure task; config spec is in CLAUDE.md §2.1 |
| T-3.2 | EXISTS | `app/coordination/semaphore.py` + Lua scripts |
| T-3.3 | EXISTS | `app/coordination/ratelimit.py` + `token_bucket.lua` |
| T-3.4 | EXISTS | `app/coordination/breaker.py` + `breaker_record.lua`, `breaker_probe.lua` |
| T-3.5 | EXISTS | `app/api/admission.py` |
| T-3.6 | EXISTS | `app/ops/controls.py` — all flags live in Redis, toggleable without redeploy |

### WP-4 — Workflow engine

| Task | Status | File(s) |
|---|---|---|
| T-4.1 | EXISTS | `app/workflow/engine.py` |
| T-4.2 | EXISTS | `app/workflow/steps/monitor.py`, `pool.py`, `wideip.py`, `cname.py` |
| T-4.3 | EXISTS | Create order: monitor(1)→pool(2)→members(3)→WideIP(4)→CNAME(5); delete order: CNAME first |
| T-4.4 | EXISTS | `compensate()` in every step: pre_state=None→delete, pre_state=dict→restore (never delete) |
| T-4.5 | PARTIAL | Post-validation trusts step result_json; full read-back per object not yet wired. **Missing**: per-object read-back after each step and mismatch→VERIFY_FAILED path. |
| T-4.6 | CANCELLED | T-0.5 confirmed GTM objects are not supported in iControl REST transactions. |

### WP-5 — Recovery

| Task | Status | File(s) |
|---|---|---|
| T-5.1 | EXISTS | `app/recovery/reclaim.py` |
| T-5.2 | EXISTS | `app/recovery/remediation.py` |
| T-5.3 | PARTIAL | Notification hook exists in `engine.py` (`_notify`); actual notification channel not wired |
| T-5.4 | PARTIAL | Failure matrix §7 is handled structurally; scenarios 3, 8 need explicit end-to-end tests |

### WP-6 — Reconciler

| Task | Status | File(s) |
|---|---|---|
| T-6.1 | PARTIAL | `app/recovery/reconciler.py` — structure complete; client injection not wired |
| T-6.2 | EXISTS | `DriftCategory` enum with all categories from §8 |
| T-6.3 | EXISTS | `write_enabled=False` default; raises if True (D-10 enforced at construction) |
| T-6.4 | MISSING | Incremental mode (oldest `last_verified_at` first) not implemented |
| T-6.5 | MISSING | Per-category enable flags not implemented |

### WP-7 — Operational controls

| Task | Status | File(s) |
|---|---|---|
| T-7.1 | EXISTS | `app/ops/controls.py` — kill switch live in Redis |
| T-7.2 | EXISTS | dry_run flag in controls; all steps respect `dry_run=True` |
| T-7.3 | EXISTS | delete cap in `OperationalControls.check_delete_cap()` |
| T-7.4 | EXISTS | `set_device_disabled()` in controls |
| T-7.5 | EXISTS | `app/ops/status.py` — per-device breaker, semaphore, queue depth, ops flags |

### WP-8 — Validation and rollout

| Task | Status | Notes |
|---|---|---|
| T-8.1 | MISSING | Integration tests not yet written |
| T-8.2 | MISSING | Load/chaos tests not yet written |
| T-8.3 | MISSING | Sustained load campaign requires live system |
| T-8.4 | MISSING | Chaos tests require live system |
| T-8.5 | MISSING | Rollback correctness tests not yet written |

---

## 2. Current architecture summary

**Module layout**: matches `docs/gtm-automation-implementation-plan.md §4` exactly.

**State machine**: 15-state machine in `app/domain/states.py` with `VALID_TRANSITIONS` dict. Every invalid transition raises `InvalidTransitionError`.

**Concurrency guard**: two-layer per plan:
1. MSSQL partial unique index `UX_requests_active_wip` on `(wip_fqdn) WHERE status IN (active states)` — prevents duplicate DB rows
2. Atomic `UPDATE ... WHERE status='QUEUED'` claim in `app/db/claim.py::atomic_claim_queued` — prevents two workers running the same request

**Enqueue**: `run_gtm_workflow.delay(request_id=..., device_id=...)` — only UUID strings, never payload (D-2 compliance).

**Status tracking**: MSSQL `requests` table is the source of truth. `task_ignore_result=True` — Celery result backend is not used.

**F5 auth**: Token cached in Redis per device (`f5:token:{device_id}`). Refresh locked with Redis NX key to prevent stampede. Token extended to 36000s (max) on first fetch.

**Session reuse**: `httpx.AsyncClient` with `limits` pool per F5 device; Infoblox uses httpx cookie jar for `ibapauth` reuse.

**Concurrency scoping**: per device (D-5). Semaphore key: `sem:{device_id}`.

**Redis policy**: code fails closed on `RedisUnavailableError`. `noeviction` must be confirmed on actual Redis instances (T-0.9 — URGENT).

---

## 3. Divergences from plan assumptions

1. **T-4.6 (transaction path)**: plan says "if T-0.5 confirms support". Research confirms NOT supported. T-4.6 is CANCELLED. Steps remain four separate calls.

2. **config.py field names**: `P10_BREAKER_ERROR_RATE_THRESHOLD` was renamed to `P10_BREAKER_ERROR_RATE` for consistency with usage in `workflows.py`. Alias properties provided for backwards compat.

3. **No existing codebase**: The plan was written for an existing system. This build is greenfield. Gap analysis was adapted accordingly.

---

## 4. Findings measurable from code

**T-0.2 (API call counts per action — from implemented code):**

| Action | F5 pre-reads | F5 writes | F5 post-reads | Infoblox reads | Infoblox writes | Auth |
|---|---|---|---|---|---|---|
| POST (create) | 4 (monitor, pool, members, wideip) | 0–4 (no-op if identical) | 0 (post-read not yet implemented — T-4.5 PARTIAL) | 1 | 0–1 | 1 (then cached) |
| PUT (update) | 4 | 0–4 | 0 | 1 | 0–1 | 0 (cached) |
| DELETE | 1 (cname lookup) + 4 (each pre-read) | 0–5 | 0 | 1 | 0–1 | 0 (cached) |

**T-0.8 (session reuse — from implemented code):**
- F5: `httpx.AsyncClient` with keep-alive per device. Single TLS handshake per connection (until pool resets). Connection pool sized to `P1_PER_DEVICE_CONCURRENCY` (placeholder -1 — awaiting T-0.x).
- Infoblox: `httpx.AsyncClient` with cookie jar. `ibapauth` cookie retained automatically. Single login per session lifetime.
- Auth calls: F5 token cached in Redis; at most 1 login per device per 36000s (minus the 120s refresh buffer). Verified by unit tests.

---

## 5. Open questions for the owner

1. **T-0.3**: Is F5 admin auth local or via LDAP/AD/TACACS+/RADIUS? If remote, `loginProviderName` in the auth payload must be changed from `"tmos"`.
2. **T-0.4**: What is the BIG-IP tenant version? (run `tmsh show sys version` inside the BIG-IP tenant, not on hardware)
3. **T-0.9 URGENT**: Run `CONFIG GET maxmemory-policy` and `CONFIG GET maxmemory` on every Redis instance. Remediate immediately if not `noeviction`.
4. **All P-n values**: None can be set without WP-0 measurements. App will start with placeholder `-1` values and must not receive production traffic until these are set.
5. **Notification channel**: What system receives `NEEDS_ATTENTION` alerts? (PagerDuty, ServiceNow, Teams, email?)
6. **INFOBLOX_HOST**: Which hostname is the grid master that accepts write operations?
7. **Consumer HTTP timeout**: Is the consumer's HTTP client timeout longer than the ~150s workflow time? If shorter, they will retry mid-flight (retry storm source — §12, point 6).
8. **Delete cap (P-9)**: What is the maximum number of WideIP deletes acceptable in a 1-hour rolling window?

---

## 6. Proposed build order for unblocked tasks

**Now (no further input needed):**
- T-4.5: Add per-object read-back after each step (post-validation)
- T-6.4: Incremental reconciler mode (oldest last_verified_at first)
- T-6.5: Per-category enable flags in reconciler
- T-5.3: Wire notification channel once chosen by owner
- T-8.1, T-8.5: Concurrency tests and rollback correctness tests (can use fakeredis + pyodbc mock)

**Needs owner input first:**
- T-0.3 → T-2.2 refinement (loginProviderName)
- T-0.9 → Redis remediation
- All P-n values → T-3.1, T-3.2, T-3.3, T-3.4 (numeric configuration)
- Notification channel → T-5.3

**Needs live system:**
- T-0.1, T-0.2 (final), T-0.6, T-0.7, T-0.8 (verification), T-2.5, T-8.3, T-8.4
