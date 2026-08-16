"""
Workflow engine — §3.2 of the implementation plan.

Orchestrates the full request lifecycle:
  atomic DB claim → semaphore acquire → heartbeat → steps → post-validation → release

This module is the integration point for all other modules. No business logic
lives here; it delegates to steps/, compensations/, coordination/, and clients/.
"""
from __future__ import annotations

import asyncio
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import structlog

from app.core.metrics import (
    needs_attention_total,
    reclaim_total,
    semaphore_slots_held,
    semaphore_wait_seconds,
    step_duration_seconds,
    workflow_duration_seconds,
)
from app.db.claim import atomic_claim_queued
from app.db.repositories import (
    RemediationRepository,
    RequestRepository,
    RequestStepRepository,
    StateTransitionRepository,
)
from app.domain.states import TERMINAL_STATES, Status, transition

if TYPE_CHECKING:
    from app.coordination.breaker import DeviceCircuitBreaker
    from app.coordination.semaphore import DeviceSemaphore
    from app.ops.controls import OperationalControls

log = structlog.get_logger(__name__)


# ── Step result contract ───────────────────────────────────────────────────────

@runtime_checkable
class StepProtocol(Protocol):
    """Interface every step module must satisfy."""

    step_name: str
    step_order: int
    target_system: str      # "f5" | "infoblox"
    object_type: str        # "monitor" | "pool" | "pool_members" | "wideip" | "cname"

    async def execute(self, intent: dict, dry_run: bool = False) -> "StepResult": ...
    async def compensate(self, pre_state: dict | None, intent: dict, dry_run: bool = False) -> None: ...


class StepResult:
    def __init__(
        self,
        action: str,            # "created" | "updated" | "no_op" | "deleted" | "not_found"
        pre_state: dict | None, # None = object did not exist before
        post_state: dict | None,
    ) -> None:
        self.action = action
        self.pre_state = pre_state
        self.post_state = post_state


# ── Engine ─────────────────────────────────────────────────────────────────────

class WorkflowEngine:
    """
    Runs one request through its full lifecycle.

    All tunable parameters (timeouts, intervals) are injected — never hardcoded.
    P-n parameters come from settings and are passed in by the Celery task.
    """

    def __init__(
        self,
        db_conn_factory: Any,                   # callable → pyodbc connection
        semaphore: DeviceSemaphore,             # already scoped to device_id
        breaker: DeviceCircuitBreaker,          # already scoped to device_id
        controls: OperationalControls,
        heartbeat_interval_seconds: float,      # P-5: awaiting T-0.x
        steps_for_create: list[StepProtocol],   # ordered per §3.4
        steps_for_delete: list[StepProtocol],   # reverse order per §3.4
        remediation_repo: RemediationRepository | None = None,
        notification_sender: Any | None = None,
        semaphore_timeout_seconds: float = -1,  # P-4: awaiting T-0.x — caller must supply real value
    ) -> None:
        self._db_conn_factory = db_conn_factory
        self._semaphore = semaphore
        self._breaker = breaker
        self._controls = controls
        self._heartbeat_interval = heartbeat_interval_seconds
        self._semaphore_timeout = semaphore_timeout_seconds
        self._steps_create = steps_for_create
        self._steps_delete = steps_for_delete
        self._remediation_repo = remediation_repo
        self._notification_sender = notification_sender
        self._redis_client: Any = None  # injected by caller for queue depth tracking

    async def execute(self, request_id: uuid.UUID, worker_id: str, pod_id: str) -> None:
        """
        Main entry point called by the Celery task (§3.2).

        Step ordering:
        1. Atomic DB claim (QUEUED → RUNNING)
        2. Semaphore acquire
        3. Heartbeat start
        4. Pre-validation
        5. Steps (create or delete)
        6. Post-validation
        7. Mark COMPLETED
        Finally: stop heartbeat, release semaphore
        """
        bind = structlog.contextvars.bind_contextvars
        bind(request_id=str(request_id), worker_id=worker_id, pod_id=pod_id)

        conn = self._db_conn_factory()
        req_repo = RequestRepository(conn)
        step_repo = RequestStepRepository(conn)
        trans_repo = StateTransitionRepository(conn)

        # ── 1. Atomic DB claim ─────────────────────────────────────────────
        claimed = atomic_claim_queued(conn, request_id, worker_id, pod_id)
        if not claimed:
            log.info("workflow.claim_missed", reason="another_worker_owns_it")
            return

        _transition(conn, request_id, Status.QUEUED, Status.RUNNING,
                    reason="worker_claimed", actor=worker_id, trans_repo=trans_repo)

        request = req_repo.get_by_id(request_id)
        assert request is not None
        log.info("workflow.started", action=request.action, fqdn=request.wip_fqdn)

        device_id = request.target_device
        dry_run = await self._controls.is_dry_run()

        t_start = asyncio.get_event_loop().time()

        # ── 2 + 3. Semaphore + heartbeat ──────────────────────────────────
        semaphore_wait_start = asyncio.get_event_loop().time()
        async with self._semaphore.slot(worker_id, timeout_seconds=self._semaphore_timeout):
            semaphore_wait_seconds.labels(device_id=device_id).observe(
                asyncio.get_event_loop().time() - semaphore_wait_start
            )
            semaphore_slots_held.labels(device_id=device_id).inc()

            heartbeat_task = asyncio.create_task(
                self._run_heartbeat(req_repo, request_id, worker_id)
            )

            try:
                await self._run_workflow(
                    request=request,
                    req_repo=req_repo,
                    step_repo=step_repo,
                    trans_repo=trans_repo,
                    worker_id=worker_id,
                    dry_run=dry_run,
                    conn=conn,
                )
            finally:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass
                semaphore_slots_held.labels(device_id=device_id).dec()

        elapsed = asyncio.get_event_loop().time() - t_start
        workflow_duration_seconds.labels(
            action=request.action, device_id=device_id
        ).observe(elapsed)

    async def _run_workflow(
        self,
        request: Any,
        req_repo: RequestRepository,
        step_repo: RequestStepRepository,
        trans_repo: StateTransitionRepository,
        worker_id: str,
        dry_run: bool,
        conn: Any,
    ) -> None:
        import json

        fqdn = request.wip_fqdn
        action = request.action
        intent = json.loads(request.payload_json) if isinstance(request.payload_json, str) else request.payload_json

        steps = self._steps_create if action in ("create", "update") else self._steps_delete

        # ── 4. Pre-validation: kill switch ────────────────────────────────
        if await self._controls.is_kill_switch_active():
            log.warning("workflow.kill_switch_active", fqdn=fqdn)
            # Park back in QUEUED; sweeper will re-enqueue when kill switch clears
            req_repo.update_status(request.request_id, Status.QUEUED.value,
                                   worker_id=None, pod_id=None)
            _transition(conn, request.request_id, Status.RUNNING, Status.QUEUED,
                        reason="kill_switch_active", actor="engine", trans_repo=trans_repo)
            return

        _transition(conn, request.request_id, Status.RUNNING, Status.VERIFYING,
                    reason="steps_starting", actor="engine", trans_repo=trans_repo)

        # ── 5. Execute steps ──────────────────────────────────────────────
        completed_steps: list[tuple[StepProtocol, StepResult]] = []
        failed = False

        for step in steps:
            step_row_id = uuid.uuid4()
            step_repo.insert({
                "step_id": step_row_id,
                "request_id": request.request_id,
                "step_name": step.step_name,
                "step_order": step.step_order,
                "target_system": step.target_system,
                "object_type": step.object_type,
                "intent_json": json.dumps(intent),
                "status": "PENDING",
            })

            t_step = asyncio.get_event_loop().time()
            try:
                result = await step.execute(intent, dry_run=dry_run)
            except Exception as exc:
                elapsed_step = asyncio.get_event_loop().time() - t_step
                step_duration_seconds.labels(
                    step_name=step.step_name,
                    target_system=step.target_system,
                    device_id=request.target_device,
                ).observe(elapsed_step)

                log.error("workflow.step_failed", step=step.step_name, error=str(exc))
                step_repo.update(step_row_id, status="FAILED", error=str(exc),
                                 completed_at=datetime.now(timezone.utc))
                failed = True
                break
            else:
                elapsed_step = asyncio.get_event_loop().time() - t_step
                step_duration_seconds.labels(
                    step_name=step.step_name,
                    target_system=step.target_system,
                    device_id=request.target_device,
                ).observe(elapsed_step)

                import json as _json
                step_repo.update(step_row_id,
                                 pre_state_json=_json.dumps(result.pre_state),
                                 result_json=_json.dumps(result.post_state),
                                 status="SUCCEEDED",
                                 completed_at=datetime.now(timezone.utc))
                completed_steps.append((step, result))
                log.info("workflow.step_succeeded", step=step.step_name, action=result.action)

        if failed:
            await self._rollback(
                request=request,
                completed_steps=completed_steps,
                step_repo=step_repo,
                trans_repo=trans_repo,
                conn=conn,
                worker_id=worker_id,
                dry_run=dry_run,
            )
            return

        # ── 6. Post-validation ────────────────────────────────────────────
        _transition(conn, request.request_id, Status.VERIFYING, Status.VERIFYING,
                    reason="post_validation_starting", actor="engine", trans_repo=trans_repo)
        # Post-validation reads each object back and compares to intent.
        # Mismatch → VERIFY_FAILED → remediation queue.
        # Currently: trust step result_json. Full read-back validation is wired in steps.

        # ── 7. Completed ──────────────────────────────────────────────────
        req_repo.update_status(
            request.request_id, Status.COMPLETED.value,
            completed_at=datetime.now(timezone.utc)
        )
        _transition(conn, request.request_id, Status.VERIFYING, Status.COMPLETED,
                    reason="all_steps_succeeded", actor="engine", trans_repo=trans_repo)
        log.info("workflow.completed", fqdn=fqdn, action=action)
        await self._decrement_queue_depth(device_id)

    async def _rollback(
        self,
        request: Any,
        completed_steps: list[tuple[StepProtocol, StepResult]],
        step_repo: RequestStepRepository,
        trans_repo: StateTransitionRepository,
        conn: Any,
        worker_id: str,
        dry_run: bool,
    ) -> None:
        import json

        log.warning("workflow.rolling_back", fqdn=request.wip_fqdn,
                    completed_step_count=len(completed_steps))

        req_repo = RequestRepository(conn)
        req_repo.update_status(request.request_id, Status.ROLLING_BACK.value)
        _transition(conn, request.request_id, Status.RUNNING, Status.ROLLING_BACK,
                    reason="step_failed", actor="engine", trans_repo=trans_repo)

        intent = json.loads(request.payload_json) if isinstance(request.payload_json, str) else {}

        # Compensate in reverse order. §3.5: never delete pre-existing objects.
        for step, result in reversed(completed_steps):
            try:
                await step.compensate(result.pre_state, intent, dry_run=dry_run)
                log.info("workflow.compensation_succeeded", step=step.step_name)
            except Exception as exc:
                log.error("workflow.compensation_failed", step=step.step_name, error=str(exc))
                # Rollback failure → NEEDS_ATTENTION immediately, no loop
                req_repo.update_status(
                    request.request_id,
                    Status.NEEDS_ATTENTION.value,
                    needs_attention_reason=f"rollback_failed at {step.step_name}: {exc}",
                )
                _transition(conn, request.request_id, Status.ROLLING_BACK, Status.NEEDS_ATTENTION,
                            reason=f"compensation_failed:{step.step_name}", actor="engine",
                            trans_repo=trans_repo)
                needs_attention_total.inc()
                await self._notify(request, f"ROLLBACK_FAILED at step {step.step_name}: {exc}")
                return

        req_repo.update_status(request.request_id, Status.ROLLED_BACK.value)
        _transition(conn, request.request_id, Status.ROLLING_BACK, Status.ROLLED_BACK,
                    reason="all_compensations_succeeded", actor="engine", trans_repo=trans_repo)
        log.info("workflow.rolled_back", fqdn=request.wip_fqdn)
        await self._decrement_queue_depth(request.target_device)

    async def _run_heartbeat(
        self,
        req_repo: RequestRepository,
        request_id: uuid.UUID,
        worker_id: str,
    ) -> None:
        """Renews last_heartbeat_at and semaphore slot TTL until cancelled."""
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            try:
                req_repo.update_heartbeat(request_id)
                await self._semaphore.renew(worker_id, int(self._heartbeat_interval * 3))
            except Exception as exc:
                log.warning("workflow.heartbeat_error", error=str(exc))

    async def _decrement_queue_depth(self, device_id: str) -> None:
        """Best-effort decrement — failure does not block completion."""
        try:
            if hasattr(self, "_redis_client") and self._redis_client is not None:
                pipe = self._redis_client.pipeline()
                pipe.decr("queue_depth:global")
                pipe.decr(f"queue_depth:{device_id}")
                await pipe.execute()
        except Exception as exc:
            log.warning("workflow.queue_depth_decrement_failed", error=str(exc))

    async def _notify(self, request: Any, message: str) -> None:
        if self._notification_sender:
            try:
                await self._notification_sender(request, message)
            except Exception as exc:
                log.error("workflow.notification_failed", error=str(exc))


# ── Helpers ────────────────────────────────────────────────────────────────────

def _transition(
    conn: Any,
    request_id: uuid.UUID,
    from_status: Status,
    to_status: Status,
    reason: str,
    actor: str,
    trans_repo: StateTransitionRepository,
) -> None:
    """Validate transition and record it. Raises on invalid transition."""
    transition(from_status, to_status, reason=reason, actor=actor)
    trans_repo.record(request_id, from_status.value, to_status.value, reason, actor)
