"""
FastAPI route handlers — POST/PUT/DELETE + status endpoint.

The API is synchronous and fast (§3.1): validate → admit → claim DB row →
enqueue Celery task → return 202. No F5 or Infoblox calls here.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status

from app.api.admission import AdmissionResult, run_admission_checks
from app.api.idempotency import compute_idempotency_key
from app.api.schemas import (
    ErrorResponse,
    GTMAction,
    StatusResponse,
    WideIPRequest,
    WideIPResponse,
)
from app.core.config import settings
from app.core.logging import bind_request_id
from app.core.metrics import needs_attention_total, request_total
from app.db.claim import atomic_insert_and_claim
from app.db.repositories import RequestRepository, StateTransitionRepository
from app.domain.states import Status, transition
from app.ops.controls import OperationalControls
from app.tasks.workflows import run_gtm_workflow

log = structlog.get_logger(__name__)

router = APIRouter()


# ── Dependency: Redis client ───────────────────────────────────────────────────

async def get_redis(request: Request):
    return request.app.state.redis


async def get_controls(request: Request) -> OperationalControls:
    return request.app.state.controls


async def get_db_conn(request: Request):
    return request.app.state.db_pool.connect()


# ── POST /wideip — Create ──────────────────────────────────────────────────────

@router.post(
    "/wideip",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=WideIPResponse,
    responses={
        400: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
)
async def create_wideip(
    body: WideIPRequest,
    redis=Depends(get_redis),
    controls: OperationalControls = Depends(get_controls),
    conn=Depends(get_db_conn),
) -> WideIPResponse:
    return await _handle_request(
        action=GTMAction.CREATE,
        body=body,
        redis=redis,
        controls=controls,
        conn=conn,
    )


@router.put(
    "/wideip",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=WideIPResponse,
)
async def update_wideip(
    body: WideIPRequest,
    redis=Depends(get_redis),
    controls: OperationalControls = Depends(get_controls),
    conn=Depends(get_db_conn),
) -> WideIPResponse:
    return await _handle_request(
        action=GTMAction.UPDATE,
        body=body,
        redis=redis,
        controls=controls,
        conn=conn,
    )


@router.delete(
    "/wideip",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=WideIPResponse,
)
async def delete_wideip(
    body: WideIPRequest,
    redis=Depends(get_redis),
    controls: OperationalControls = Depends(get_controls),
    conn=Depends(get_db_conn),
) -> WideIPResponse:
    return await _handle_request(
        action=GTMAction.DELETE,
        body=body,
        redis=redis,
        controls=controls,
        conn=conn,
    )


# ── GET /wideip/{request_id} — Status ─────────────────────────────────────────

@router.get(
    "/wideip/{request_id}",
    response_model=StatusResponse,
    responses={404: {"model": ErrorResponse}},
)
async def get_status(
    request_id: str,
    conn=Depends(get_db_conn),
) -> StatusResponse:
    try:
        req_uuid = uuid.UUID(request_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid request_id format")

    repo = RequestRepository(conn)
    row = repo.get_by_id(req_uuid)
    if row is None:
        raise HTTPException(status_code=404, detail="Request not found")

    return StatusResponse(
        request_id=str(row.request_id),
        status=row.status,
        wip_fqdn=row.wip_fqdn,
        target_device=row.target_device,
        action=row.action,
        created_at=row.created_at,
        updated_at=row.updated_at,
        started_at=row.started_at,
        completed_at=row.completed_at,
        last_error=row.last_error,
    )


# ── Shared request handler ─────────────────────────────────────────────────────

async def _handle_request(
    action: GTMAction,
    body: WideIPRequest,
    redis,
    controls: OperationalControls,
    conn,
) -> WideIPResponse:
    """
    Implements §3.1 API path steps 1–9.

    This function must complete in milliseconds. No F5/Infoblox calls.
    """
    import json

    request_id = str(uuid.uuid4())
    bind_request_id(request_id)

    log.info("api.request_received", action=action, fqdn=body.wip_fqdn, device=body.target_device)

    # ── Step 2: Schema already validated by Pydantic ──────────────────────

    # ── Step 3: Resolve target device ────────────────────────────────────
    target_device = body.target_device
    if not settings.is_known_device(target_device):
        request_total.labels(action=action, status="rejected_unknown_device").inc()
        raise HTTPException(status_code=400, detail=f"Unknown target device: {target_device}")

    # ── Step 4: Compute idempotency key ──────────────────────────────────
    idempotency_key = (
        body.idempotency_key
        or compute_idempotency_key(action.value, body.wip_fqdn, body.payload)
    )

    # ── Step 5: Admission checks ──────────────────────────────────────────
    admission: AdmissionResult = await run_admission_checks(
        redis_client=redis,
        controls=controls,
        target_device=target_device,
        global_queue_limit=settings.P7_GLOBAL_QUEUE_DEPTH_LIMIT,    # TODO: awaiting T-0.x
        device_queue_limit=settings.P8_PER_DEVICE_QUEUE_DEPTH_LIMIT, # TODO: awaiting T-0.x
    )
    if not admission.allowed:
        request_total.labels(action=action, status="rejected_admission").inc()
        raise HTTPException(
            status_code=admission.status_code or 503,
            detail=admission.error or "Service unavailable",
            headers={"Retry-After": str(admission.retry_after or 30)},
        )

    # ── Step 6: Atomic DB claim ───────────────────────────────────────────
    payload_json = json.dumps(body.payload)
    import hashlib
    payload_hash = hashlib.sha256(payload_json.encode()).hexdigest()

    new_request_dict = {
        "request_id": uuid.UUID(request_id),
        "idempotency_key": idempotency_key,
        "action": action.value,
        "wip_fqdn": body.wip_fqdn,
        "target_device": target_device,
        "payload_hash": payload_hash,
        "payload_json": payload_json,
        "status": Status.RECEIVED.value,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    }

    created, existing_or_new = atomic_insert_and_claim(conn, new_request_dict)

    if not created:
        existing = existing_or_new
        if existing.idempotency_key == idempotency_key:
            # D-9: same idempotency key → return 200 with original request
            request_total.labels(action=action, status="idempotent_replay").inc()
            log.info("api.idempotent_replay", existing_request_id=str(existing.request_id))
            return WideIPResponse(
                request_id=str(existing.request_id),
                status=existing.status,
                status_url=f"/wideip/{existing.request_id}",
                retry_after=30,
            )
        else:
            # D-8: different key → 409 conflict
            request_total.labels(action=action, status="conflict").inc()
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "FQDN already has an active request",
                    "request_id": str(existing.request_id),
                    "status": existing.status,
                    "action": existing.action,
                },
            )

    new_row = existing_or_new
    repo = RequestRepository(conn)
    trans_repo = StateTransitionRepository(conn)

    # ── Step 7: RECEIVED → QUEUED ─────────────────────────────────────────
    transition(Status.RECEIVED, Status.QUEUED, reason="api_accepted", actor="api")
    repo.update_status(new_row.request_id, Status.QUEUED.value, updated_at=datetime.now(timezone.utc))
    trans_repo.record(new_row.request_id, Status.RECEIVED.value, Status.QUEUED.value,
                      "api_accepted", "api")

    # ── Step 8: Enqueue — request_id ONLY (D-2) ───────────────────────────
    run_gtm_workflow.delay(
        request_id=str(new_row.request_id),
        device_id=target_device,
    )

    # Increment queue depth counters so admission checks see live depth.
    # These are best-effort — a Redis failure here does not block the 202.
    try:
        pipe = redis.pipeline()
        pipe.incr("queue_depth:global")
        pipe.incr(f"queue_depth:{target_device}")
        await pipe.execute()
    except Exception:
        pass

    request_total.labels(action=action, status="accepted").inc()
    log.info("api.request_queued", new_request_id=str(new_row.request_id))

    # ── Step 9: Return 202 ───────────────────────────────────────────────
    return WideIPResponse(
        request_id=str(new_row.request_id),
        status=Status.QUEUED.value,
        status_url=f"/wideip/{new_row.request_id}",
        retry_after=30,
    )
