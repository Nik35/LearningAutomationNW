"""
Prometheus metric definitions for the GTM automation service.

All metrics are module-level singletons, created once at import time and
shared across the process.  In multi-process Celery deployments, metrics
that accumulate per-worker are declared with ``multiprocess_mode`` where
appropriate (prometheus-client reads this from PROMETHEUS_MULTIPROC_DIR).

Metric taxonomy (§10 of the implementation plan)
-------------------------------------------------
- Request lifecycle  : request_total, workflow_duration_seconds, step_duration_seconds
- External calls     : f5_call_*, infoblox_call_*
- Coordination       : semaphore_*, bucket_*, breaker_state, queue_depth
- Recovery           : reclaim_total, remediation_depth, needs_attention_total
- Drift              : drift_detected_total

Labels marked "per device" use a ``device_id`` label so Prometheus can
fan-out per F5 grid.

Do NOT import config here — metrics are defined before app startup so that
any import-time errors surface immediately.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# ---------------------------------------------------------------------------
# Request lifecycle
# ---------------------------------------------------------------------------

request_total: Counter = Counter(
    "gtm_requests_total",
    "Total provisioning requests received, by action and final status.",
    ["action", "status"],
)

workflow_duration_seconds: Histogram = Histogram(
    "gtm_workflow_duration_seconds",
    "End-to-end workflow duration from RECEIVED to terminal state.",
    ["action", "device_id"],
)

step_duration_seconds: Histogram = Histogram(
    "gtm_step_duration_seconds",
    "Duration of each workflow step (pre-read, write, post-read combined).",
    ["step_name", "target_system", "device_id"],
)

# ---------------------------------------------------------------------------
# External call metrics
# ---------------------------------------------------------------------------

f5_call_duration_seconds: Histogram = Histogram(
    "gtm_f5_call_duration_seconds",
    "Latency of individual F5 iControl REST API calls.",
    ["operation", "device_id"],
)

f5_call_errors_total: Counter = Counter(
    "gtm_f5_call_errors_total",
    "F5 iControl REST API errors, by operation, device and error type.",
    ["operation", "device_id", "error_type"],
)

infoblox_call_duration_seconds: Histogram = Histogram(
    "gtm_infoblox_call_duration_seconds",
    "Latency of individual Infoblox WAPI calls.",
    ["operation"],
)

infoblox_call_errors_total: Counter = Counter(
    "gtm_infoblox_call_errors_total",
    "Infoblox WAPI errors, by operation and error type.",
    ["operation", "error_type"],
)

# ---------------------------------------------------------------------------
# Coordination — semaphore, token bucket, circuit breaker, queue
# ---------------------------------------------------------------------------

semaphore_slots_held: Gauge = Gauge(
    "gtm_semaphore_slots_held",
    "Number of per-device semaphore slots currently in use.",
    ["device_id"],
)

semaphore_wait_seconds: Histogram = Histogram(
    "gtm_semaphore_wait_seconds",
    "Time (seconds) a worker spent waiting to acquire a semaphore slot.",
    ["device_id"],
)

bucket_rejections_total: Counter = Counter(
    "gtm_bucket_rejections_total",
    "Number of requests rejected by the per-device token bucket.",
    ["device_id"],
)

breaker_state: Gauge = Gauge(
    "gtm_breaker_state",
    "Circuit breaker state per device (0 = closed, 1 = half_open, 2 = open).",
    ["device_id"],
)

queue_depth: Gauge = Gauge(
    "gtm_queue_depth",
    "Current number of requests queued for a device (QUEUED status in MSSQL).",
    ["device_id"],
)

# ---------------------------------------------------------------------------
# Recovery
# ---------------------------------------------------------------------------

reclaim_total: Counter = Counter(
    "gtm_reclaim_total",
    "Number of stale RUNNING requests reclaimed by the sweeper.",
)

remediation_depth: Gauge = Gauge(
    "gtm_remediation_depth",
    "Number of items currently in the remediation queue.",
)

needs_attention_total: Counter = Counter(
    "gtm_needs_attention_total",
    "Cumulative count of requests that entered NEEDS_ATTENTION state.",
)

# ---------------------------------------------------------------------------
# Drift
# ---------------------------------------------------------------------------

drift_detected_total: Counter = Counter(
    "gtm_drift_detected_total",
    "Number of drift items detected by the reconciler, by category.",
    ["category"],
)
