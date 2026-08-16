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
