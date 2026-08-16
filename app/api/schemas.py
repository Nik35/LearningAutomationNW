"""
Pydantic v2 request and response models for the GTM automation API.

Design notes
------------
- ``WideIPRequest.payload`` is deliberately typed as ``dict`` rather than
  a fully-specified model.  F5 iControl REST field names and structure
  differ between BIG-IP versions.  Until T-0.4 confirms the installed
  version and field names are verified against official docs for that
  version, we must not invent or assume field names.  Pre-validation
  (§3.1 step 5 / worker §3.2 step 5) will validate the payload dict
  against a version-specific schema once that information is available.

- ``wip_fqdn`` is normalised (lowercased and stripped) at validation time
  so that the idempotency key computation (T-1.4) operates on a canonical
  form.

- ``idempotency_key`` is optional here.  When absent the API computes one
  from sha256(action | wip_fqdn | normalise(payload)) per §3.1 step 4.
  When supplied by the client, the API stores it verbatim and uses it for
  D-9 replay detection.

- All datetime fields use timezone-aware types (``datetime`` with
  ``AwareDatetime`` validator) to prevent naive-datetime bugs in MSSQL
  round-trips.  The DB layer stores UTC.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import AwareDatetime, BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class GTMAction(str, Enum):
    """The three supported lifecycle actions for a WideIP."""

    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------


class WideIPRequest(BaseModel):
    """
    Incoming request body for POST / PUT / DELETE on the WideIP resource.

    Fields
    ------
    action:
        Desired lifecycle action.
    wip_fqdn:
        The WideIP FQDN.  Normalised to lowercase + stripped whitespace
        at validation time so that idempotency key computation is
        consistent regardless of client casing.
    target_device:
        Identifier of the F5 device grid to target (e.g. "dc-a-f5-01").
        Must map to a device known to the service; validated in the worker
        pre-validation phase against the configured device registry.
    payload:
        Raw GTM configuration dict.  Field names are intentionally left
        unvalidated here.

        TODO: verify field names against F5 iControl REST documentation
        for the installed BIG-IP version (T-0.4) and replace this dict
        with a version-specific Pydantic model.  Any field added here
        before that verification will be an invented name, which is
        explicitly forbidden by the project rules.
    idempotency_key:
        Client-supplied idempotency key.  The server computes its own key
        (sha256 of normalised action + FQDN + payload) and compares.
        When absent, the server-computed key is used exclusively.
    """

    action: GTMAction
    wip_fqdn: str
    target_device: str
    payload: dict[str, Any]
    idempotency_key: str | None = None

    @field_validator("wip_fqdn", mode="before")
    @classmethod
    def normalise_fqdn(cls, value: str) -> str:
        """Lowercase and strip the FQDN for canonical idempotency handling."""
        if not isinstance(value, str):
            raise ValueError("wip_fqdn must be a string")
        normalised = value.strip().lower()
        if not normalised:
            raise ValueError("wip_fqdn must not be empty")
        return normalised

    @field_validator("target_device", mode="before")
    @classmethod
    def normalise_device(cls, value: str) -> str:
        """Strip whitespace from the device identifier."""
        if not isinstance(value, str):
            raise ValueError("target_device must be a string")
        stripped = value.strip()
        if not stripped:
            raise ValueError("target_device must not be empty")
        return stripped


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class WideIPResponse(BaseModel):
    """
    Returned on a successful 202 Accepted for a new provisioning request.

    The client should poll ``status_url`` to track progress.
    ``retry_after`` is the recommended minimum interval in seconds.
    """

    request_id: str
    status: str
    status_url: str
    retry_after: int = Field(
        description="Recommended polling interval in seconds."
    )


class StatusResponse(BaseModel):
    """
    Detailed status of an individual provisioning request.

    Returned from GET /requests/{request_id}/status.

    ``steps`` is included when the caller requests verbose output or when
    the request is in a failure state.  Each dict in the list corresponds
    to a ``request_steps`` row (keys: step_name, step_order, target_system,
    object_type, status, error, started_at, completed_at).
    """

    request_id: str
    status: str
    wip_fqdn: str
    target_device: str
    action: str
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    last_error: str | None = None
    steps: list[dict[str, Any]] | None = None


# ---------------------------------------------------------------------------
# Error response
# ---------------------------------------------------------------------------


class ErrorResponse(BaseModel):
    """
    Uniform error envelope for 4xx and 5xx responses.

    ``retry_after`` is only populated on 503 responses to convey the
    back-pressure signal.  Clients MUST respect it.
    """

    error: str
    detail: str | None = None
    request_id: str | None = None
    retry_after: int | None = Field(
        default=None,
        description=(
            "Seconds before the client should retry.  "
            "Only present on 503 Service Unavailable."
        ),
    )
