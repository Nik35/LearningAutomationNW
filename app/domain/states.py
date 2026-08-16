"""
State machine for GTM automation requests.

Implements §3.6 of the implementation plan exactly.  Every call to
`transition()` validates the edge before it is acted on; the caller is
responsible for writing the resulting row to `state_transitions`.
"""

from __future__ import annotations

from enum import Enum


class Status(str, Enum):
    """All possible lifecycle states for a provisioning request."""

    RECEIVED = "RECEIVED"
    VALIDATING = "VALIDATING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    VERIFYING = "VERIFYING"
    COMPLETED = "COMPLETED"
    VERIFY_FAILED = "VERIFY_FAILED"
    REMEDIATING = "REMEDIATING"
    FAILED = "FAILED"
    ROLLING_BACK = "ROLLING_BACK"
    ROLLED_BACK = "ROLLED_BACK"
    ROLLBACK_FAILED = "ROLLBACK_FAILED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    NEEDS_ATTENTION = "NEEDS_ATTENTION"


# ---------------------------------------------------------------------------
# Transition table — the single authoritative source of allowed edges.
# Any (current, next) pair not in this dict is forbidden.
# ---------------------------------------------------------------------------

VALID_TRANSITIONS: dict[Status, frozenset[Status]] = {
    Status.RECEIVED: frozenset(
        {Status.VALIDATING, Status.REJECTED, Status.NEEDS_ATTENTION}
    ),
    Status.VALIDATING: frozenset(
        {Status.QUEUED, Status.REJECTED, Status.NEEDS_ATTENTION}
    ),
    Status.QUEUED: frozenset(
        {Status.RUNNING, Status.CANCELLED, Status.NEEDS_ATTENTION}
    ),
    Status.RUNNING: frozenset(
        {Status.VERIFYING, Status.FAILED, Status.NEEDS_ATTENTION}
    ),
    Status.VERIFYING: frozenset(
        {Status.COMPLETED, Status.VERIFY_FAILED, Status.NEEDS_ATTENTION}
    ),
    Status.VERIFY_FAILED: frozenset(
        {Status.REMEDIATING, Status.NEEDS_ATTENTION}
    ),
    Status.REMEDIATING: frozenset(
        {Status.COMPLETED, Status.NEEDS_ATTENTION}
    ),
    Status.FAILED: frozenset(
        {Status.ROLLING_BACK, Status.NEEDS_ATTENTION}
    ),
    Status.ROLLING_BACK: frozenset(
        {Status.ROLLED_BACK, Status.ROLLBACK_FAILED, Status.NEEDS_ATTENTION}
    ),
    # Terminal states — no outbound edges except escalation to NEEDS_ATTENTION.
    # NEEDS_ATTENTION itself is strictly terminal (nothing exits it automatically).
    Status.ROLLED_BACK: frozenset({Status.NEEDS_ATTENTION}),
    Status.ROLLBACK_FAILED: frozenset({Status.NEEDS_ATTENTION}),
    Status.COMPLETED: frozenset(),
    Status.CANCELLED: frozenset(),
    Status.REJECTED: frozenset(),
    Status.NEEDS_ATTENTION: frozenset(),
}

# ---------------------------------------------------------------------------
# State sets used by concurrency guards and admission checks.
# ---------------------------------------------------------------------------

# States that hold the unique filtered index concurrency guard (UX_requests_active_wip).
# Must match the WHERE clause in the DDL exactly.
ACTIVE_STATES: frozenset[Status] = frozenset(
    {
        Status.RECEIVED,
        Status.VALIDATING,
        Status.QUEUED,
        Status.RUNNING,
        Status.VERIFYING,
    }
)

# States from which no further automatic progression occurs.
TERMINAL_STATES: frozenset[Status] = frozenset(
    {
        Status.COMPLETED,
        Status.CANCELLED,
        Status.REJECTED,
        Status.ROLLED_BACK,
        Status.ROLLBACK_FAILED,
        Status.NEEDS_ATTENTION,
    }
)


# ---------------------------------------------------------------------------
# Exception
# ---------------------------------------------------------------------------


class InvalidTransitionError(Exception):
    """Raised when a requested state transition is not in VALID_TRANSITIONS."""

    def __init__(
        self,
        current: Status,
        next_state: Status,
        reason: str | None = None,
    ) -> None:
        self.current = current
        self.next_state = next_state
        super().__init__(
            f"Invalid transition {current.value!r} → {next_state.value!r}"
            + (f": {reason}" if reason else "")
        )


# ---------------------------------------------------------------------------
# Guard function
# ---------------------------------------------------------------------------


def transition(
    current: Status,
    next_state: Status,
    reason: str = "",
    actor: str = "",
) -> None:
    """
    Validate that *current* → *next_state* is a permitted edge.

    Raises
    ------
    InvalidTransitionError
        If the transition is not listed in VALID_TRANSITIONS.

    Notes
    -----
    This function only validates.  The caller must persist the transition
    by writing a row to `state_transitions` **and** updating
    `requests.status` in the same DB transaction.

    Parameters
    ----------
    current:
        The present state of the request.
    next_state:
        The desired new state.
    reason:
        Human-readable explanation (stored in `state_transitions.reason`).
    actor:
        Identity of the component driving the transition
        (stored in `state_transitions.actor`), e.g. ``"api"``,
        ``"worker:<worker_id>"``, ``"reclaim_sweeper"``.
    """
    allowed = VALID_TRANSITIONS.get(current, frozenset())
    if next_state not in allowed:
        raise InvalidTransitionError(current, next_state, reason or None)
