# CLAUDE.md

Project rules for the F5 GTM automation service. These apply to every session.

## What this is

A FastAPI + Celery + Redis + MSSQL service that provisions GSLB configuration on
F5 BIG-IP DNS (WideIP, pool, pool members, monitor) and CNAME records in Infoblox,
in response to API calls from an OpenShift-based consumer.

**This is production infrastructure.** Every OpenShift deployment depends on it.
Deployed across 2 datacentres, 4 pods, targeting 4 independent F5 grids.

Implementation plan: `docs/gtm-automation-implementation-plan.md`. Read it before
making structural changes.

## Hard rules

### Never invent a `P-n` parameter value

All load-governing numbers — concurrency limits, token bucket size and refill rate,
circuit breaker thresholds, queue depth limits, timeouts — come from the WP-0
measurements in §9 of the plan.

If a measurement has not been supplied, leave a named config constant with a
`# TODO: awaiting T-0.x` comment and an obvious placeholder. Do not pick a
reasonable-looking number. A wrong value here silently overloads production F5 devices.

### Never assume an F5 or Infoblox API shape

Endpoints, field names and response formats differ between versions. Confirm against
official F5 iControl REST or Infoblox WAPI documentation for the installed version,
or against a live dev call, before writing client code. Never write plausible-looking
API code from memory.

### Every operation must be idempotent

Read current state → compare to desired → act only if different → **no-op if identical**
→ never error on the second run. The no-op branch is mandatory.

### Rollback must never delete pre-existing objects

Capture `pre_state_json` before every step. On rollback:
- object did not exist beforehand → delete it
- object did exist beforehand → restore prior state, never delete

A failed update must not destroy something that was there before the request arrived.

### Timeouts are not failures

A timeout means the outcome is **unknown**. Never blind-retry — read back to determine
actual state, then converge.

### The reconciler does not delete

Report-only. This estate carries inherited drift from a previous Ansible implementation.
Auto-deletion is off by default and enabled per-category only, deliberately.

### Feature flags, always

The system is live. Existing behaviour keeps working until each new component is
explicitly enabled. No behaviour-changing refactors in one step.

## Architecture invariants

- **MSSQL is the sole source of truth.** Redis is a dispatch mechanism only.
- **Enqueue `request_id` only**, never the payload.
- **Concurrency is scoped per target device**, never globally — the 4 prod F5 devices
  are separate grids and are independent capacity pools.
- **Redis unavailable → fail closed.** Reject new work with 503 + `Retry-After`.
  Never proceed without limits.
- **`maxmemory-policy` must be `noeviction`** on every Redis instance. Any `allkeys-*`
  policy silently deletes queued Celery tasks.
- **The API never calls F5 or Infoblox.** All external work happens in workers.
- **Two concurrency guards, both required**: the unique filtered index on active
  `wip_fqdn`, and the atomic `QUEUED → RUNNING` claim in the worker.
- **Object order**: create `monitor → pool → members → WideIP → CNAME`;
  delete in reverse. CNAME must be removed before the WideIP.

## Recovery

- `NEEDS_ATTENTION` is terminal. Nothing automatic exits it. Entry raises a notification.
- The reclaim sweeper only reclaims `RUNNING` rows whose heartbeat is stale past the
  threshold. A slow worker with a healthy heartbeat is never reclaimed — reclaiming it
  would produce two concurrent writers on the same WideIP.
- Failed steps go to the remediation queue with backoff, then escalate. Nothing is
  silently dropped.

## Working style

Direct and concise. Reasoning before implementation, not after. Prefer simple solutions
and flag over-engineering. State uncertainty plainly rather than producing
confident-sounding output.

Ask rather than guess about existing table shapes, function signatures, or config
structures.
