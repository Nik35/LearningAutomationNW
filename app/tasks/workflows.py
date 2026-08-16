"""
Celery tasks for GTM workflow execution.

Each task receives only a request_id (D-2: never enqueue the payload).
The worker loads the full request from MSSQL and runs the workflow engine.
"""
from __future__ import annotations

import uuid

import structlog

from app.core.config import settings
from app.core.logging import bind_request_id
from app.tasks.celery_app import celery_app

log = structlog.get_logger(__name__)


def _build_engine_for_device(device_id: str) -> object:
    """
    Construct a WorkflowEngine wired up to the correct F5 device.
    Called at task-execution time so each task gets fresh connections.

    This is the composition root for a single workflow run. All dependencies
    are assembled here and injected into the engine.
    """
    import redis.asyncio as aioredis

    from app.clients.f5.auth import F5TokenManager
    from app.clients.f5.gtm import F5GTMClient
    from app.clients.f5.session import F5Session
    from app.clients.infoblox.records import InfobloxClient
    from app.clients.infoblox.session import InfobloxSession
    from app.coordination.breaker import DeviceCircuitBreaker
    from app.coordination.semaphore import DeviceSemaphore
    from app.db.repositories import RemediationRepository
    from app.ops.controls import OperationalControls
    from app.workflow.engine import WorkflowEngine
    from app.workflow.steps.cname import CNAMEStep
    from app.workflow.steps.monitor import MonitorStep
    from app.workflow.steps.pool import PoolMembersStep, PoolStep
    from app.workflow.steps.wideip import WideIPStep

    redis_client = aioredis.from_url(settings.REDIS_URL)

    # F5 client for this device
    device_cfg = settings.get_device_config(device_id)
    f5_session = F5Session(
        device_id=device_id,
        host=device_cfg["host"],
        max_connections=settings.P1_PER_DEVICE_CONCURRENCY,   # TODO: awaiting T-0.6/T-0.7
        timeout_seconds=settings.F5_REQUEST_TIMEOUT_SECONDS,
        verify_ssl=settings.F5_VERIFY_SSL,
    )
    f5_token_mgr = F5TokenManager(
        redis_client=redis_client,
        device_id=device_id,
        username=device_cfg["username"],
        password=device_cfg["password"],
        session=f5_session,
        login_provider_name=settings.F5_LOGIN_PROVIDER_NAME,  # TACACS+ source name
    )
    f5_client = F5GTMClient(session=f5_session, token_manager=f5_token_mgr)

    # Infoblox client
    ib_session = InfobloxSession(
        host=settings.INFOBLOX_HOST,
        username=settings.INFOBLOX_USERNAME,
        password=settings.INFOBLOX_PASSWORD,
        wapi_version=settings.INFOBLOX_WAPI_VERSION,
        verify_ssl=settings.INFOBLOX_VERIFY_SSL,
        timeout_seconds=settings.INFOBLOX_REQUEST_TIMEOUT_SECONDS,
    )
    ib_client = InfobloxClient(session=ib_session)

    # Coordination
    semaphore = DeviceSemaphore(
        redis_client=redis_client,
        device_id=device_id,
        max_slots=settings.P1_PER_DEVICE_CONCURRENCY,      # TODO: awaiting T-0.6/T-0.7
        slot_ttl=int(settings.P5_HEARTBEAT_INTERVAL_SECONDS * 3),  # TODO: awaiting T-0.x
    )
    breaker = DeviceCircuitBreaker(
        redis_client=redis_client,
        device_id=device_id,
        error_rate_threshold=settings.P10_BREAKER_ERROR_RATE,        # TODO: awaiting T-0.6/T-0.7
        p95_latency_threshold_ms=settings.P10_BREAKER_P95_LATENCY_MS, # TODO: awaiting T-0.x
        consecutive_timeout_threshold=settings.P10_BREAKER_CONSECUTIVE_TIMEOUTS, # TODO: awaiting T-0.x
        window_seconds=60,
        half_open_probe_ttl=30,
    )

    controls = OperationalControls(
        redis_client=redis_client,
        delete_cap=settings.P9_MAX_DELETES_PER_WINDOW,   # TODO: business decision
    )

    # Step lists — ordered per §3.4
    steps_create = [
        MonitorStep(f5_client),
        PoolStep(f5_client),
        PoolMembersStep(f5_client),
        WideIPStep(f5_client),
        CNAMEStep(ib_client),
    ]
    # Delete is reverse; CNAME first, then WideIP, then pool/monitor
    steps_delete = [
        CNAMEStep(ib_client),
        WideIPStep(f5_client),
        PoolMembersStep(f5_client),
        PoolStep(f5_client),
        MonitorStep(f5_client),
    ]

    import pyodbc
    def db_conn_factory() -> pyodbc.Connection:
        return pyodbc.connect(settings.DB_CONNECTION_STRING)

    return WorkflowEngine(
        db_conn_factory=db_conn_factory,
        semaphore=semaphore,
        breaker=breaker,
        controls=controls,
        heartbeat_interval_seconds=settings.P5_HEARTBEAT_INTERVAL_SECONDS,  # TODO: awaiting T-0.x
        steps_for_create=steps_create,
        steps_for_delete=steps_delete,
        semaphore_timeout_seconds=settings.P4_SEMAPHORE_ACQUIRE_TIMEOUT,    # TODO: awaiting T-0.x
    )


@celery_app.task(name="app.tasks.workflows.run_gtm_workflow", bind=True)
def run_gtm_workflow(self, request_id: str, device_id: str) -> None:
    """
    Entry point for the Celery worker.

    Receives request_id (string UUID) only — never the payload (D-2).
    Loads request from MSSQL and dispatches to WorkflowEngine.
    """
    import asyncio

    req_uuid = uuid.UUID(request_id)
    bind_request_id(request_id)

    log.info("task.received", request_id=request_id, device_id=device_id)

    engine = _build_engine_for_device(device_id)

    worker_id = self.request.id or str(uuid.uuid4())
    pod_id = settings.POD_ID  # injected via env in OpenShift

    try:
        asyncio.run(engine.execute(req_uuid, worker_id=worker_id, pod_id=pod_id))
    except Exception as exc:
        log.error("task.unhandled_error", request_id=request_id, error=str(exc), exc_info=True)
        raise  # let Celery retry or fail the task
