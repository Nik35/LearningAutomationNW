"""
Admission checks for the GTM automation API (§3.1 step 5).

Checks run in the order specified by the plan — cheapest first.  The first
failing check short-circuits; subsequent checks are not executed.

Check order
-----------
a. Redis reachable?                         → 503 + Retry-After  [D-4]
b. Kill switch engaged?                     → 503 + Retry-After  [T-7.1]
c. Device disabled?                         → 503 + Retry-After  [T-7.4]
d. Global queue depth < P-7?               → 503 + Retry-After
e. Target device breaker closed OR
   device queue depth < P-8?              → 503 + Retry-After  [T-3.4]

All 503 responses carry a ``retry_after`` value so that clients implement
backpressure correctly.

P-7 and P-8 are passed as function arguments — NEVER hardcoded here.  They
come from the settings object in the caller.

Redis keys read (must match the coordination layer exactly)
-----------------------------------------------------------
    breaker:{device_id}:state   → "closed"|"half_open"|"open"
                                  Matches app/coordination/breaker.py.
    queue_depth:global           → integer string (P-7 check)
                                  TODO: confirm key name with coordination agent.
    queue_depth:{device_id}      → integer string (P-8 check)
                                  TODO: confirm key name with coordination agent.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from app.core.logging import get_logger
from app.ops.controls import OperationalControls

log = get_logger(__name__)

# Redis key templates (read-only references; the coordination layer owns writes).
# Key names must match what is written by the coordination agent:
#   breaker.py → "breaker:{device_id}:state"
# Queue depth keys: TODO — confirm naming with the coordination agent building
# the queue-depth writers.  Using a consistent prefix here for now.
_GLOBAL_QUEUE_DEPTH_KEY = "queue_depth:global"
_DEVICE_QUEUE_DEPTH_KEY = "queue_depth:{device_id}"
_BREAKER_STATE_KEY = "breaker:{device_id}:state"   # matches coordination/breaker.py

# Default Retry-After hint for 503 responses (seconds).
# Callers may override this by reading P-5 from settings, but we need a
# safe fallback that does not require a P-n value to be available.
_DEFAULT_RETRY_AFTER_SECONDS = 30


class AdmissionResult(BaseModel):
    """
    Result of the admission check sequence.

    Attributes
    ----------
    allowed:
        True when all checks pass.  The caller may proceed to claim the
        request in MSSQL.
    status_code:
        HTTP status code to return when ``allowed`` is False.
        Always 503 for admission failures.
    error:
        Short machine-readable error key, e.g. "kill_switch_active".
    retry_after:
        Seconds the client should wait before retrying.  Present only when
        ``allowed`` is False and ``status_code`` is 503.
    """

    allowed: bool
    status_code: int | None = None
    error: str | None = None
    retry_after: int | None = None


async def run_admission_checks(
    redis_client: Any,
    controls: OperationalControls,
    target_device: str,
    *,
    global_queue_limit: int,   # P-7 — NEVER hardcoded
    device_queue_limit: int,   # P-8 — NEVER hardcoded
    retry_after: int = _DEFAULT_RETRY_AFTER_SECONDS,
) -> AdmissionResult:
    """
    Run all admission checks in the plan-specified order.

    Parameters
    ----------
    redis_client:
        Open async Redis connection/pool.
    controls:
        Initialised ``OperationalControls`` instance.
    target_device:
        The F5 device identifier from the incoming request.
    global_queue_limit:
        P-7: maximum global queue depth before new work is rejected.
    device_queue_limit:
        P-8: maximum per-device queue depth before that device is rejected,
        unless the circuit breaker is closed.
    retry_after:
        Seconds to include in the Retry-After hint on 503 responses.
        Should come from settings (derived from P-5 or a business SLO),
        not hardcoded by callers.

    Returns
    -------
    AdmissionResult
        ``allowed=True`` when all checks pass; otherwise the first failure.
    """

    # ------------------------------------------------------------------
    # a. Redis reachable?
    # ------------------------------------------------------------------
    try:
        await redis_client.ping()
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "admission_redis_unreachable",
            target_device=target_device,
            error=str(exc),
        )
        return AdmissionResult(
            allowed=False,
            status_code=503,
            error="redis_unreachable",
            retry_after=retry_after,
        )

    # ------------------------------------------------------------------
    # b. Kill switch engaged?
    # ------------------------------------------------------------------
    if await controls.is_kill_switch_active():
        log.info(
            "admission_rejected_kill_switch",
            target_device=target_device,
        )
        return AdmissionResult(
            allowed=False,
            status_code=503,
            error="kill_switch_active",
            retry_after=retry_after,
        )

    # ------------------------------------------------------------------
    # c. Device disabled?
    # ------------------------------------------------------------------
    if await controls.is_device_disabled(target_device):
        log.info(
            "admission_rejected_device_disabled",
            target_device=target_device,
        )
        return AdmissionResult(
            allowed=False,
            status_code=503,
            error="device_disabled",
            retry_after=retry_after,
        )

    # ------------------------------------------------------------------
    # d. Global queue depth < P-7?
    # Pipeline with check (e) to minimise round trips.
    # ------------------------------------------------------------------
    pipe = redis_client.pipeline()
    pipe.get(_GLOBAL_QUEUE_DEPTH_KEY)
    pipe.get(_DEVICE_QUEUE_DEPTH_KEY.format(device_id=target_device))
    pipe.get(_BREAKER_STATE_KEY.format(device_id=target_device))
    raw_global, raw_device, raw_breaker = await pipe.execute()

    global_depth = int(raw_global) if raw_global else 0
    if global_depth >= global_queue_limit:
        log.info(
            "admission_rejected_global_queue_full",
            target_device=target_device,
            global_depth=global_depth,
            limit=global_queue_limit,
        )
        return AdmissionResult(
            allowed=False,
            status_code=503,
            error="global_queue_full",
            retry_after=retry_after,
        )

    # ------------------------------------------------------------------
    # e. Target device breaker closed OR device queue depth < P-8?
    #
    # Per T-3.4: "When open, requests stay QUEUED — they do not fail."
    # The plan says to reject at admission when the breaker is open AND
    # the per-device queue is also at or above P-8.  When the breaker is
    # closed, the per-device queue check still applies independently.
    # ------------------------------------------------------------------
    device_depth = int(raw_device) if raw_device else 0
    breaker_state_raw = raw_breaker
    if isinstance(breaker_state_raw, bytes):
        breaker_state_raw = breaker_state_raw.decode()
    breaker_open = breaker_state_raw == "open"

    if breaker_open or device_depth >= device_queue_limit:
        # Both conditions must be true to reject; if breaker is open but
        # device queue still has room, we admit and hold in QUEUED state
        # (per T-3.4: "requests stay QUEUED — they do not fail").
        # If breaker is closed but queue is full, we reject.
        if device_depth >= device_queue_limit:
            log.info(
                "admission_rejected_device_queue_full",
                target_device=target_device,
                device_depth=device_depth,
                limit=device_queue_limit,
                breaker_open=breaker_open,
            )
            return AdmissionResult(
                allowed=False,
                status_code=503,
                error="device_queue_full",
                retry_after=retry_after,
            )
        # Breaker is open but queue has room — admit and park in QUEUED.
        # The worker will handle the open-breaker condition.
        log.info(
            "admission_admitted_breaker_open_queue_has_room",
            target_device=target_device,
            device_depth=device_depth,
        )

    # ------------------------------------------------------------------
    # All checks passed.
    # ------------------------------------------------------------------
    log.debug(
        "admission_allowed",
        target_device=target_device,
        global_depth=global_depth,
        device_depth=device_depth,
        breaker_open=breaker_open,
    )
    return AdmissionResult(allowed=True)
