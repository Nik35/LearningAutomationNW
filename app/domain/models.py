"""
Domain objects for the GTM automation service.

Pure Pydantic v2 models — no ORM, no DB coupling.  Fields are exactly
those listed in §6 of the implementation plan.

JSON columns (payload_json, pre_state_json, etc.) are stored as
serialised strings in MSSQL and surfaced here as ``str``.  Callers that
need the parsed dict should use ``json.loads(field)``.  We keep them as
strings in the domain layer to avoid silent data loss when an arbitrary
dict passes through the system (schema validation is the caller's
responsibility).
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from app.domain.states import Status


class Request(BaseModel):
    """Top-level provisioning request — maps to the ``requests`` table."""

    model_config = ConfigDict(use_enum_values=True)

    request_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    idempotency_key: str
    action: str  # e.g. "create", "update", "delete"
    wip_fqdn: str
    target_device: str
    payload_hash: str
    payload_json: str  # JSON-serialised string
    status: Status = Status.RECEIVED

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    worker_id: Optional[str] = None
    pod_id: Optional[str] = None
    last_heartbeat_at: Optional[datetime] = None

    attempt_count: int = 0
    last_error: Optional[str] = None
    needs_attention_reason: Optional[str] = None


class RequestStep(BaseModel):
    """
    One atomic step within a workflow — maps to ``request_steps``.

    ``compensation_status`` tracks whether the rollback compensating
    action for this step has run: ``None`` means not attempted,
    ``"PENDING"``, ``"SUCCEEDED"``, ``"FAILED"``.
    """

    model_config = ConfigDict(use_enum_values=False)

    step_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    request_id: uuid.UUID

    step_name: str  # e.g. "create_monitor", "create_pool"
    step_order: int  # 1-based; defines execution and rollback ordering
    target_system: str  # "f5" | "infoblox"
    object_type: str  # e.g. "monitor", "pool", "wideip", "cname"
    object_key: str  # natural key on the target system

    intent_json: str  # JSON-serialised desired state
    pre_state_json: Optional[str] = None  # JSON-serialised state before the step
    result_json: Optional[str] = None  # JSON-serialised outcome

    status: str = "PENDING"  # PENDING | RUNNING | SUCCEEDED | FAILED | SKIPPED
    attempts: int = 0
    error: Optional[str] = None

    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    compensation_status: Optional[str] = None  # None | PENDING | SUCCEEDED | FAILED


class ManagedObject(BaseModel):
    """
    Long-lived record of a GSLB object managed by this service
    — maps to ``managed_objects``.

    Used by the reconciler to track drift between MSSQL and the
    target systems.
    """

    model_config = ConfigDict(use_enum_values=False)

    object_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    wip_fqdn: str
    object_type: str  # "monitor" | "pool" | "pool_member" | "wideip" | "cname"
    object_key: str
    target_device: str

    desired_state_json: str  # JSON-serialised

    last_verified_at: Optional[datetime] = None
    drift_detected_at: Optional[datetime] = None
    drift_details_json: Optional[str] = None  # JSON-serialised

    owning_request_id: Optional[uuid.UUID] = None
    status: str = "ACTIVE"  # ACTIVE | PENDING_DELETE | DELETED


class StateTransition(BaseModel):
    """
    Append-only audit record of every status change
    — maps to ``state_transitions``.
    """

    model_config = ConfigDict(use_enum_values=True)

    request_id: uuid.UUID
    from_status: Status
    to_status: Status
    reason: str = ""
    actor: str = ""
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class RemediationItem(BaseModel):
    """
    A failed step queued for automatic retry
    — maps to ``remediation_queue``.

    ``next_retry_at`` is computed by the enqueue logic using exponential
    backoff.  ``escalated_at`` is set when retry_count reaches the cap
    and the item transitions to NEEDS_ATTENTION.
    """

    model_config = ConfigDict(use_enum_values=False)

    remediation_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    request_id: uuid.UUID
    step_id: uuid.UUID

    failure_category: str  # e.g. "cname_failed", "post_validation_mismatch"
    retry_count: int = 0
    next_retry_at: Optional[datetime] = None
    escalated_at: Optional[datetime] = None
    resolution: Optional[str] = None  # filled on success or escalation
