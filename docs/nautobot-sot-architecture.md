# Nautobot SOT — architecture reference

Status: proposed, pending design review
Owner: Nikhil
Last updated: 20 August 2026

---

## 1. Purpose

Establish Nautobot as the single source of truth for network device and IP data,
by ingesting from ~10 upstream systems of record and exposing one governed read
interface to consumers.

Nautobot version: 3.x. REST API version pinned by clients.

### Sources (initial)

| Source | Type | Provides | Cadence |
|---|---|---|---|
| NetBrain | REST | Devices, topology, interfaces | Weekly / monthly |
| IPAM DB | MSSQL | IP addresses, subnets | Daily |
| Cisco Prime | REST | Wireless devices | Daily |
| Forescout | REST | NAC device data | Daily |
| Mist | REST | Wireless | TBC |
| (others) | TBC | TBC | TBC |

NetBrain is authoritative for device identity. All other sources enrich existing
devices; they do not create them.

### Scale

- Devices: thousands
- IP addresses and config records: up to ~1 million
- Daily change rate expected to be low single-digit percent

---

## 2. Component overview

Two services. Different shapes — this asymmetry is deliberate.

### `sot-ingest` — scheduled worker

Internal only. No ingress, no HTTP endpoints, not callable by anything.

- Celery workers + a single Celery beat process
- Beat schedule stored in the database (runtime-adjustable), seeded from
  version-controlled config on deploy
- One adapter module per source, behind a common interface
- Writes to Nautobot through the shared client
- Single pod for the worker set; beat enabled on one replica only

### `sot-api` — read facade

The only externally reachable path to Nautobot data.

- HTTP, read-only
- Auth and RBAC at this layer
- Nautobot token scoped with no write permission
- Serves inventory, device, and wireless lookups to consumers

### `nautobot-client` — shared library

Internal Python package, versioned, imported by `sot-ingest` and `sot-api`.

- Nautobot REST API version pinned here, in one place
- Auth, retry, backoff, pagination, bulk-endpoint handling
- Global write concurrency budget

### Staging database — Postgres

Separate instance from Nautobot's own database. Never shares its schema.

**This is not a second source of truth.** It is a write-ahead buffer with
history, read by the loader and by humans debugging. If anything starts querying
staging for current inventory state, that is a design violation.

---

## 3. Access control — two entry points

Nautobot is reachable only from `sot-ingest` (write) and `sot-api` (read).

Enforced by, not merely documented as:

- Network policy restricting which pods can reach Nautobot
- Distinct Nautobot API tokens per service, with different permission scopes
- The `sot-api` token has **no write scope at all**

**Known exceptions:** the Nautobot web UI and its administrators. Nautobot's own
RBAC governs UI access; API tokens govern service access.

**Open question:** whether a controlled human-correction write path lands in the
read plane later. If it does, that is a third entry point and should be named
deliberately rather than discovered.

---

## 4. Data flow

1. **Extract** — beat triggers a source adapter on its own schedule. Adapter
   fetches from the source and writes raw payloads to staging. No transformation,
   no Nautobot contact.
2. **Stage** — raw payload stored verbatim with a content hash. Run recorded in
   `sync_run`.
3. **Transform** — source vocabulary mapped to Nautobot's model. Field ownership
   matrix applied; writes from non-authoritative sources for a given field are
   dropped here.
4. **Delta** — content hash compared against last known state. Unchanged records
   skipped entirely, never sent to Nautobot.
5. **Load** — changed records written via `nautobot-client`. Outcome per record
   recorded in `load_result`.
6. **Reconcile** — periodic full pass soft-deletes records no longer present in
   an authoritative source.

Extract and load are separately retryable. A load failure does not require
re-fetching from the source.

---

## 5. Staging schema

```sql
sync_run(
  run_id uuid primary key,
  source text, entity_type text,
  started_at timestamptz, ended_at timestamptz,
  status text,                      -- running | ok | failed | partial
  records_fetched int, records_changed int,
  trigger text                      -- scheduled | manual | retry
)

raw_record(
  run_id uuid, source text, entity_type text,
  natural_key text,
  payload jsonb,
  content_hash bytea,               -- sha256 of canonicalised payload
  fetched_at timestamptz
) partition by range (fetched_at)

staged_entity(
  entity_type text, natural_key text, source text,
  attrs jsonb,                      -- normalised field names
  content_hash bytea,
  first_seen_run uuid, last_seen_run uuid,
  last_changed_at timestamptz,
  primary key (entity_type, natural_key, source)
)

load_result(
  run_id uuid, entity_type text, natural_key text,
  action text,                      -- create | update | skip | fail | soft_delete
  nautobot_id uuid, error text, at timestamptz
)
```

**Retention:** `raw_record` partitioned by date, 30 days retained, older
partitions dropped. `staged_entity` holds current state only and does not grow
with time.

---

## 6. Key mechanisms

### Content-hash delta

Canonicalise the payload (sorted keys, normalised whitespace and casing), hash,
compare against `staged_entity.content_hash`. Unchanged records are skipped
before any Nautobot call. At ~1M records with low daily churn this is the
difference between a million writes and tens of thousands. This is the core of
the performance story.

### Field ownership matrix

Version-controlled YAML, reviewed in pull requests. Not a database table —
config changes must have a review trail.

```yaml
device:
  primary_ip:   netbrain
  serial:       netbrain
  platform:     netbrain
interface:
  mac_address:  forescout
  description:  netbrain
ip_address:
  dns_name:     ipam_mssql
```

The loader drops writes from non-authoritative sources for each field. Without
this, load order silently determines the winner — last writer wins by accident
rather than by decision.

### Reconciliation and soft delete

After a successful full pass from an authoritative source, any `staged_entity`
whose `last_seen_run` is not the current run has disappeared from that source.

- Soft-delete only: status change plus a tag in Nautobot. Never hard delete.
- **Sanity floor:** abort the reconciliation if the run returned less than ~80%
  of the previous run's record count. Prevents a flaky API from soft-deleting
  half the estate.

### Job preconditions

Cadence is handled by independent beat schedules, not by a chained workflow. The
dependency between sources still exists — it has moved into the clock, and clocks
drift.

Each job asserts its prerequisites at start, reading from `sync_run`:

- Enrichment jobs (Prime, Forescout, Mist) require a successful NetBrain run
  within N days
- IP loading requires interfaces to exist

Abort loudly on failure. This converts "silently created orphaned records" into
an obvious error.

### Job locking

Redis `SET NX PX` keyed on job name. Protects against a slow run overlapping the
next scheduled run of the same job, and against concurrent writes to the same
Nautobot objects.

### Write concurrency

A global write budget shared across all adapters, enforced in
`nautobot-client`. Nautobot's ORM-backed API is not fast; without a shared
budget, overlapping jobs can degrade the SOT.

**Open: the concrete number has not been set.**

---

## 7. Monitoring

- **Celery Flower** — task execution, failures, worker health
- **`sync_run` alerting** — alert when the last successful run for a source
  exceeds its expected interval

The second is not redundant. Flower shows tasks that ran; it cannot show a task
that was never scheduled (beat entry disabled, schedule edited, worker not
listening on the queue). Silent absence is the failure mode, and `sync_run` is
also the durable record Flower's ephemeral history cannot provide.

---

## 8. Decisions and rejected alternatives

### Ingestion runs outside Nautobot, not as SSoT plugins

**Rejected:** Nautobot's native SSoT app (DiffSync + Jobs), which provides a sync
dashboard, diff logging, dry-run, safe-delete, per-object sync metadata, and
direct ORM writes.

**Reason:** plugins and Nautobot form a single upgrade unit — Nautobot cannot be
upgraded until every adapter is ported and tested. An external service pinning a
REST API version decouples the two release cadences. With three engineers, that
scheduling independence outweighs the framework's built-in features.

**Honest cost:** sync history, dry-run, safe-delete and provenance metadata must
be rebuilt (Sections 5–6 cover this). ORM-speed writes become REST-speed writes.

**Note:** major-version data model changes break both approaches equally —
Nautobot 2.0 removed the Region and Site models, breaking REST clients and
plugins alike. API versioning protects the serialisation contract, not the
underlying model. This is a data-model risk, not an architecture risk.

### No Kafka

**Rejected:** adapters produce to Kafka, a core service consumes and writes.

**Reason:** Kafka earns its place with multiple independent consumers, replay
across time, backpressure between fast producers and slow consumers, or genuine
event streams. This workload has none — batch sync, one destination, one
consumer. The durability and replay arguments are served by staging tables at a
fraction of the operational cost.

### Adapters write to Nautobot directly, not through a core service API

**Rejected:** adapters call a core service's REST API, which calls Nautobot's
REST API.

**Reason:** wrapping a REST API in a REST API for bulk writes means double
serialisation, double auth, a throughput bottleneck on the write path, and
reimplementing Nautobot's bulk semantics. The legitimate concern behind the
proposal — duplicated client logic across adapters — is solved by a shared
library, not a shared service.

### Celery beat, not a workflow engine

**Rejected:** Temporal (already deployed elsewhere in the estate) owning the DAG.

**Reason:** workflow engines earn their keep when steps must be coordinated
within a single run — fan-out, retry-from-step-N, mid-chain state. NetBrain is
weekly/monthly; other sources are daily and independent. These are separate
schedules, not a chain. Day 0 population is manual and sequential before release,
so initial ordering is not a system problem.

**Mitigation:** adapters are plain Python functions with no Celery-specific logic.
The orchestration layer is a thin shim, so replacing it later is contained.

**Revisit if:** cadence tightens materially, a single source grows long enough to
need mid-run recovery, or genuine cross-source coordination appears within one run.

### Grouped adapter deployment

Adapters are modules behind a common interface, not one deployable per source.
Isolation, where genuinely needed, comes from separate Celery queues and worker
pools — not separate deployments. Ten deployables for three engineers means ten
pipelines, ten image builds, ten alert configurations.

---

## 9. Open items

Ordered by severity.

1. **Natural key resolution per entity type.** What identifies the same device
   across NetBrain, Prime, and Forescout? Hostname is unreliable (case, FQDN vs
   short name, domain suffix). Serial is stable but not universally reported.
   Getting this wrong creates duplicate objects in Nautobot that surface weeks
   later. Decide per entity type, normalise aggressively on ingest, and resolve
   all non-authoritative sources against NetBrain's key.

2. **Field ownership matrix.** Reported as already decided by the team — not yet
   captured in this document. Needs to be written down in the YAML form above
   before load code is written.

3. **`sot-api` design.** Endpoints, contract, pagination, caching, auth model,
   and whether it ships day 1 or phase 2. This is half the system and the half
   that faces users; it has had no design attention so far.

4. **Write concurrency budget.** A number, not a principle.

5. **Reconciliation schedule.** How often the full pass runs per source, and the
   exact soft-delete tag and status conventions in Nautobot.

6. **Team ramp.** Two of three engineers are new to the organisation. How they
   become productive on this codebase, and who owns which sources.

---

## 10. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Natural key collision or mismatch | Duplicate objects in the SOT, discovered late | Resolve open item 1 before load code; add duplicate detection to the load stage |
| Source API instability | Stale data, failed runs | Staging allows re-transform without re-fetch; precondition checks fail loudly |
| Nautobot write throughput at ~1M records | Long load windows, SOT degradation | Content-hash delta; shared write budget; bulk endpoints |
| Silent job non-execution | Undetected data staleness | `sync_run` interval alerting |
| Bad reconciliation run | Mass soft-delete | Sanity floor on record count; soft delete only, never hard |
| Small team, two new joiners | Bus factor, slow ramp | Shared library and common adapter interface keep per-source work uniform |

---

## 11. Deployment summary

| Unit | Kind | Replicas | Ingress |
|---|---|---|---|
| `sot-ingest` workers | Celery worker pod | 2–3 | None |
| `sot-ingest` beat | Celery beat | 1 (strictly) | None |
| `sot-api` | HTTP service | 2 | External, authenticated |
| Staging Postgres | Database | Managed | None |
| Nautobot 3 | Existing | Existing | UI only |

Exactly one beat process must run. Two beats means every task fires twice.
