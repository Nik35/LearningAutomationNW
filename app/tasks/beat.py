"""
Celery beat schedule — periodic maintenance tasks.

Single beat instance, auto-restarting (as today per the plan).
"""
from __future__ import annotations

import structlog

from app.core.config import settings
from app.tasks.celery_app import celery_app

log = structlog.get_logger(__name__)


@celery_app.task(name="app.tasks.beat.reclaim_stale_workers")
def reclaim_stale_workers() -> None:
    """Reclaim RUNNING rows with stale heartbeats and orphaned QUEUED rows."""
    import asyncio
    import pyodbc

    from app.core.config import settings
    from app.recovery.reclaim import WorkerReclaimer
    from app.tasks.celery_app import celery_app as app

    def db_conn_factory():
        return pyodbc.connect(settings.DB_CONNECTION_STRING)

    reclaimer = WorkerReclaimer(
        db_conn_factory=db_conn_factory,
        celery_app=app,
        heartbeat_stale_threshold=settings.P6_STALE_HEARTBEAT_THRESHOLD,  # TODO: awaiting T-0.x
    )
    result = asyncio.run(reclaimer.run())
    log.info("beat.reclaim_complete", **result)


@celery_app.task(name="app.tasks.beat.process_remediation_queue")
def process_remediation_queue() -> None:
    """Retry failed steps that are due for another attempt."""
    import asyncio
    import pyodbc

    from app.recovery.remediation import RemediationWorker

    def db_conn_factory():
        return pyodbc.connect(settings.DB_CONNECTION_STRING)

    worker = RemediationWorker(
        db_conn_factory=db_conn_factory,
        celery_app=celery_app,
        notification_sender=None,  # TODO: wire up notification channel
    )
    processed = asyncio.run(worker.process_due_items())
    log.info("beat.remediation_complete", processed=processed)


@celery_app.task(name="app.tasks.beat.run_reconciler")
def run_reconciler() -> None:
    """Drift detection sweep — report-only (D-10)."""
    import asyncio
    import pyodbc

    from app.recovery.reconciler import Reconciler

    def db_conn_factory():
        return pyodbc.connect(settings.DB_CONNECTION_STRING)

    # Clients are injected as None here — reconciler uses Protocol stubs.
    # Full wiring happens when clients are ready and the reconciler is enabled.
    reconciler = Reconciler(
        db_conn_factory=db_conn_factory,
        f5_clients={},      # TODO: wire per-device F5 clients
        infoblox_client=None,  # TODO: wire Infoblox client
        write_enabled=False,
    )
    # result = asyncio.run(reconciler.run_sweep())
    log.info("beat.reconciler_skipped", reason="clients_not_wired")


# ── Beat schedule ──────────────────────────────────────────────────────────────

celery_app.conf.beat_schedule = {
    "reclaim-stale-workers": {
        "task": "app.tasks.beat.reclaim_stale_workers",
        "schedule": 30.0,   # every 30 seconds; adjust to ≤ P-5 interval
    },
    "process-remediation-queue": {
        "task": "app.tasks.beat.process_remediation_queue",
        "schedule": 60.0,   # every minute
    },
    "run-reconciler": {
        "task": "app.tasks.beat.run_reconciler",
        "schedule": 3600.0,  # hourly off-peak; adjust when reconciler is wired
    },
}
