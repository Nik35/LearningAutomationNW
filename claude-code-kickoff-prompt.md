# Claude Code kickoff prompt

Paste the block below into Claude Code as your first message, after placing
`gtm-automation-implementation-plan.md` in the repo (suggested: `docs/`).

Adjust the two paths on the first line if yours differ.

---

## PROMPT — copy from here

You are working on a production F5 GTM automation service at `E:\Training-Nikhil`. The implementation plan is at `docs/gtm-automation-implementation-plan.md`. Read it fully before doing anything else.

This system provisions GSLB configuration on F5 BIG-IP DNS and CNAME records in Infoblox, in response to API calls from an OpenShift-based consumer. It is already in dev and under active use. Every OpenShift deployment depends on it. Treat it as production code, not a greenfield project.

### Phase 0 — gap analysis. Do not write any code yet.

Read the codebase and the plan. Produce `docs/gap-analysis.md` containing:

1. **Task-by-task status.** For every task in the plan (T-0.1 through T-8.5), state one of: `EXISTS`, `PARTIAL`, `MISSING`, or `BLOCKED`. For `EXISTS` and `PARTIAL`, give file paths and line references. For `PARTIAL`, state specifically what is missing. For `BLOCKED`, state which measurement or decision it waits on.

2. **Current architecture summary.** What actually exists today: module layout, table DDL, Celery configuration, how the concurrency cap is currently enforced, how requests are enqueued (is the payload passed to `.delay()` or just an ID?), how status is tracked, what the existing sweeper does, how F5 and Infoblox clients are structured, and how sessions/auth are handled.

3. **Divergences.** Anywhere the code contradicts an assumption in the plan. The plan was written from a verbal description, so expect some. Flag them explicitly rather than silently adapting.

4. **Findings you can measure from code alone.** Complete T-0.2 (count actual outbound API calls per POST, PUT and DELETE — split into F5 pre-validation reads, F5 writes, F5 post-validation reads, Infoblox reads, Infoblox writes, and auth calls) and T-0.8 (session reuse, cookie reuse, TLS renegotiation per call, connection pool sizing). Report actual numbers with file references.

5. **Questions.** Everything you need answered before implementation. Be specific.

6. **Proposed build order** for the tasks that are unblocked, with dependencies stated.

Stop after this. Do not begin implementation until the gap analysis is reviewed.

### Hard rules — these apply for the entire project

**1. Never invent a value for a `P-n` parameter.** Every load-governing number — concurrency limits, token bucket size and refill rate, breaker thresholds, queue depth limits, timeouts — comes from the WP-0 measurements in §9 of the plan. If a measurement has not been supplied, leave the value as a named configuration constant with a `# TODO: awaiting T-0.x` comment and a value that is obviously a placeholder. Do not pick something reasonable-looking. A wrong number here silently overloads production F5 devices.

**2. Never assume an F5 or Infoblox API shape.** Endpoints, field names, and response formats differ between versions. Before writing any client code, confirm against official F5 iControl REST or Infoblox WAPI documentation for the installed version, or against a live dev call. If you cannot confirm, write the code with the uncertainty marked and list it as a question. Do not write plausible-looking API code from memory.

**3. Ask rather than assume about existing code.** If you need to know the shape of an existing table, function, or config structure and cannot determine it from the repo, ask. Do not guess and build on the guess.

**4. Incremental changes behind feature flags.** This system is live. Existing behaviour must keep working until each new component is explicitly enabled. No large refactors that change behaviour in one step.

**5. Every step must be idempotent.** Any operation against F5 or Infoblox must be safely re-runnable: read current state, compare to desired, act only if different, no-op if identical, and never error on the second run. The no-op branch is mandatory, not optional.

**6. Rollback must never delete pre-existing objects.** Every step captures `pre_state_json` before acting. On rollback, objects that did not exist beforehand are deleted; objects that did exist are restored to their prior state. A failed update must never destroy something that was there before the request arrived.

**7. Timeouts are not failures.** A timeout from F5 or Infoblox means the outcome is unknown, not that the operation failed. Never blind-retry after a timeout — read back to determine actual state first, then converge.

**8. The reconciler must not delete anything.** Report-only at launch. This estate has inherited drift from a previous Ansible implementation, and an auto-deleting reconciler on its first production run is the worst possible failure mode.

### What you can build without further input

WP-1 (schema, state machine, idempotency keys), WP-3 (Redis coordination primitives — semaphore, token bucket, circuit breaker, all atomic via Lua), WP-4 (workflow engine, step framework, compensation framework), WP-7 (kill switch, dry-run, destructive caps), and test suites for all of these.

Their structure does not depend on the WP-0 measurements — only their configured numeric values do.

### What is blocked pending input

All of WP-0. WP-2's client layer, which must be verified against dev devices. Every `P-n` value. The AS3 and transaction-path decisions (T-0.4, T-0.5). Do not work around these.

### Subagent usage

Parallel agents editing the same files will conflict. If you use subagents, split strictly by module boundary and only after the interfaces between them are defined and agreed:

- `db/` + `domain/` — schema, repositories, state machine
- `coordination/` — Redis Lua primitives
- `ops/` — kill switch, dry-run, caps, status endpoint

Anything touching `workflow/engine.py` runs sequentially, because every other module's interface depends on its shape. Define the engine's interfaces first, then parallelise around it.

### Working style

Direct and concise. Explain the reasoning behind design choices before implementing them, not after. Prefer simple solutions; flag anything that feels over-engineered. When you are uncertain, say so plainly rather than producing confident-sounding output.

## PROMPT — copy to here
