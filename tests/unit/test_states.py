"""
Unit tests for app.domain.states.

Coverage:
  - Every valid transition succeeds (no exception raised)
  - Every invalid transition raises InvalidTransitionError
  - TERMINAL_STATES and ACTIVE_STATES membership
  - transition() argument pass-through (reason / actor stored on the exception)
"""

from __future__ import annotations

import pytest

from app.domain.states import (
    ACTIVE_STATES,
    TERMINAL_STATES,
    VALID_TRANSITIONS,
    InvalidTransitionError,
    Status,
    transition,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _all_statuses() -> list[Status]:
    return list(Status)


# ---------------------------------------------------------------------------
# Valid transitions — every edge in VALID_TRANSITIONS must succeed.
# ---------------------------------------------------------------------------


class TestValidTransitions:
    """transition() must not raise for any permitted edge."""

    @pytest.mark.parametrize(
        "current, next_state",
        [
            # Happy path
            (Status.RECEIVED, Status.VALIDATING),
            (Status.VALIDATING, Status.QUEUED),
            (Status.QUEUED, Status.RUNNING),
            (Status.RUNNING, Status.VERIFYING),
            (Status.VERIFYING, Status.COMPLETED),
            # Verify-failed path
            (Status.VERIFYING, Status.VERIFY_FAILED),
            (Status.VERIFY_FAILED, Status.REMEDIATING),
            (Status.REMEDIATING, Status.COMPLETED),
            # Failure + rollback
            (Status.RUNNING, Status.FAILED),
            (Status.FAILED, Status.ROLLING_BACK),
            (Status.ROLLING_BACK, Status.ROLLED_BACK),
            (Status.ROLLING_BACK, Status.ROLLBACK_FAILED),
            # Cancellation
            (Status.QUEUED, Status.CANCELLED),
            # Early rejection
            (Status.RECEIVED, Status.REJECTED),
            (Status.VALIDATING, Status.REJECTED),
            # NEEDS_ATTENTION from every state that allows it
            (Status.RECEIVED, Status.NEEDS_ATTENTION),
            (Status.VALIDATING, Status.NEEDS_ATTENTION),
            (Status.QUEUED, Status.NEEDS_ATTENTION),
            (Status.RUNNING, Status.NEEDS_ATTENTION),
            (Status.VERIFYING, Status.NEEDS_ATTENTION),
            (Status.VERIFY_FAILED, Status.NEEDS_ATTENTION),
            (Status.REMEDIATING, Status.NEEDS_ATTENTION),
            (Status.FAILED, Status.NEEDS_ATTENTION),
            (Status.ROLLING_BACK, Status.NEEDS_ATTENTION),
            (Status.ROLLED_BACK, Status.NEEDS_ATTENTION),
            (Status.ROLLBACK_FAILED, Status.NEEDS_ATTENTION),
        ],
    )
    def test_valid_edge_does_not_raise(
        self, current: Status, next_state: Status
    ) -> None:
        # Should be a no-op (returns None)
        result = transition(current, next_state, reason="test", actor="pytest")
        assert result is None

    def test_valid_transitions_dict_is_complete(self) -> None:
        """Every Status must appear as a key in VALID_TRANSITIONS."""
        missing = [s for s in Status if s not in VALID_TRANSITIONS]
        assert missing == [], f"Missing from VALID_TRANSITIONS: {missing}"

    def test_valid_transitions_values_are_frozensets_of_status(self) -> None:
        for state, allowed in VALID_TRANSITIONS.items():
            for s in allowed:
                assert isinstance(s, Status), (
                    f"VALID_TRANSITIONS[{state!r}] contains non-Status value {s!r}"
                )


# ---------------------------------------------------------------------------
# Invalid transitions — every (current, next) pair NOT in VALID_TRANSITIONS.
# We test a representative sample rather than the full O(n²) matrix.
# ---------------------------------------------------------------------------


class TestInvalidTransitions:
    """transition() must raise InvalidTransitionError for forbidden edges."""

    @pytest.mark.parametrize(
        "current, next_state",
        [
            # Cannot go backward
            (Status.VALIDATING, Status.RECEIVED),
            (Status.QUEUED, Status.VALIDATING),
            (Status.RUNNING, Status.QUEUED),
            (Status.VERIFYING, Status.RUNNING),
            (Status.COMPLETED, Status.RUNNING),
            # Cannot skip states
            (Status.RECEIVED, Status.RUNNING),
            (Status.RECEIVED, Status.COMPLETED),
            (Status.RECEIVED, Status.QUEUED),
            (Status.QUEUED, Status.COMPLETED),
            (Status.QUEUED, Status.VERIFYING),
            # Terminal states have no exits (except NEEDS_ATTENTION)
            (Status.COMPLETED, Status.RECEIVED),
            (Status.COMPLETED, Status.VALIDATING),
            (Status.COMPLETED, Status.QUEUED),
            (Status.COMPLETED, Status.NEEDS_ATTENTION),
            (Status.CANCELLED, Status.RUNNING),
            (Status.REJECTED, Status.QUEUED),
            (Status.NEEDS_ATTENTION, Status.RUNNING),
            (Status.NEEDS_ATTENTION, Status.COMPLETED),
            # Rollback-specific
            (Status.RECEIVED, Status.ROLLING_BACK),
            (Status.VALIDATING, Status.ROLLING_BACK),
            (Status.QUEUED, Status.ROLLING_BACK),
            (Status.VERIFYING, Status.ROLLING_BACK),
            # Wrong paths
            (Status.ROLLED_BACK, Status.COMPLETED),
            (Status.ROLLBACK_FAILED, Status.COMPLETED),
        ],
    )
    def test_invalid_edge_raises(self, current: Status, next_state: Status) -> None:
        with pytest.raises(InvalidTransitionError) as exc_info:
            transition(current, next_state)
        err = exc_info.value
        assert err.current == current
        assert err.next_state == next_state

    def test_all_non_edges_raise(self) -> None:
        """Exhaustive: every (current, next) not in VALID_TRANSITIONS raises."""
        for current in Status:
            allowed = VALID_TRANSITIONS[current]
            for next_state in Status:
                if next_state in allowed:
                    continue
                with pytest.raises(InvalidTransitionError):
                    transition(current, next_state)

    def test_error_message_contains_state_names(self) -> None:
        with pytest.raises(InvalidTransitionError) as exc_info:
            transition(Status.COMPLETED, Status.RUNNING)
        msg = str(exc_info.value)
        assert "COMPLETED" in msg
        assert "RUNNING" in msg

    def test_error_carries_reason(self) -> None:
        with pytest.raises(InvalidTransitionError) as exc_info:
            transition(Status.COMPLETED, Status.RUNNING, reason="because test")
        assert "because test" in str(exc_info.value)

    def test_error_attributes(self) -> None:
        with pytest.raises(InvalidTransitionError) as exc_info:
            transition(Status.NEEDS_ATTENTION, Status.RECEIVED)
        err = exc_info.value
        assert err.current == Status.NEEDS_ATTENTION
        assert err.next_state == Status.RECEIVED


# ---------------------------------------------------------------------------
# TERMINAL_STATES
# ---------------------------------------------------------------------------


class TestTerminalStates:
    def test_completed_is_terminal(self) -> None:
        assert Status.COMPLETED in TERMINAL_STATES

    def test_cancelled_is_terminal(self) -> None:
        assert Status.CANCELLED in TERMINAL_STATES

    def test_rejected_is_terminal(self) -> None:
        assert Status.REJECTED in TERMINAL_STATES

    def test_rolled_back_is_terminal(self) -> None:
        assert Status.ROLLED_BACK in TERMINAL_STATES

    def test_rollback_failed_is_terminal(self) -> None:
        assert Status.ROLLBACK_FAILED in TERMINAL_STATES

    def test_needs_attention_is_terminal(self) -> None:
        assert Status.NEEDS_ATTENTION in TERMINAL_STATES

    def test_active_states_not_in_terminal(self) -> None:
        overlap = ACTIVE_STATES & TERMINAL_STATES
        assert overlap == frozenset(), f"States in both sets: {overlap}"

    def test_terminal_states_have_no_non_attention_exits(self) -> None:
        """
        Once a request is terminal, it must not be able to proceed to any
        non-NEEDS_ATTENTION state (NEEDS_ATTENTION itself is the only
        allowed escalation from some terminal states, and it is itself terminal).
        """
        for state in TERMINAL_STATES:
            exits = VALID_TRANSITIONS[state] - {Status.NEEDS_ATTENTION}
            assert exits == frozenset(), (
                f"Terminal state {state!r} has unexpected exits: {exits!r}"
            )


# ---------------------------------------------------------------------------
# ACTIVE_STATES
# ---------------------------------------------------------------------------


class TestActiveStates:
    """ACTIVE_STATES must match the partial index WHERE clause in the DDL."""

    _EXPECTED = frozenset(
        {
            Status.RECEIVED,
            Status.VALIDATING,
            Status.QUEUED,
            Status.RUNNING,
            Status.VERIFYING,
        }
    )

    def test_active_states_match_index_where_clause(self) -> None:
        assert ACTIVE_STATES == self._EXPECTED, (
            "ACTIVE_STATES has diverged from the DDL partial index WHERE clause. "
            "Update both together to keep the concurrency guard correct."
        )

    def test_received_is_active(self) -> None:
        assert Status.RECEIVED in ACTIVE_STATES

    def test_validating_is_active(self) -> None:
        assert Status.VALIDATING in ACTIVE_STATES

    def test_queued_is_active(self) -> None:
        assert Status.QUEUED in ACTIVE_STATES

    def test_running_is_active(self) -> None:
        assert Status.RUNNING in ACTIVE_STATES

    def test_verifying_is_active(self) -> None:
        assert Status.VERIFYING in ACTIVE_STATES

    def test_completed_not_active(self) -> None:
        assert Status.COMPLETED not in ACTIVE_STATES

    def test_failed_not_active(self) -> None:
        assert Status.FAILED not in ACTIVE_STATES


# ---------------------------------------------------------------------------
# transition() return value
# ---------------------------------------------------------------------------


class TestTransitionReturnValue:
    def test_returns_none_on_success(self) -> None:
        result = transition(Status.RECEIVED, Status.VALIDATING)
        assert result is None

    def test_accepts_empty_reason_and_actor(self) -> None:
        # Should not raise when optional args are omitted.
        transition(Status.RECEIVED, Status.VALIDATING, reason="", actor="")

    def test_accepts_only_reason(self) -> None:
        transition(Status.RECEIVED, Status.VALIDATING, reason="validation started")

    def test_accepts_only_actor(self) -> None:
        transition(Status.RECEIVED, Status.VALIDATING, actor="api")
