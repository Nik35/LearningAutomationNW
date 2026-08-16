"""
Base step contract and shared utilities.

Every step module must subclass BaseStep and implement execute() + compensate().
The §3.3 pattern (read → compare → act → no-op → verify) is enforced here.
"""
from __future__ import annotations

import abc
import json
from dataclasses import dataclass
from typing import Literal

ActionTaken = Literal["created", "updated", "no_op", "deleted", "not_found"]


@dataclass
class StepResult:
    action: ActionTaken
    pre_state: dict | None   # None = object did not exist before this call
    post_state: dict | None  # None = object now absent


class BaseStep(abc.ABC):
    """
    Abstract base for all workflow steps.

    Subclasses must declare: step_name, step_order, target_system, object_type
    and implement execute() and compensate().
    """

    step_name: str
    step_order: int
    target_system: str
    object_type: str

    @abc.abstractmethod
    async def execute(self, intent: dict, dry_run: bool = False) -> StepResult:
        """
        Idempotent operation. Must follow §3.3:
        1. READ current state → pre_state
        2. Compare desired vs actual
        3. ACT only if different; no-op if identical
        4. Verify (read-back) after write
        Return StepResult with pre_state captured before any write.
        """

    @abc.abstractmethod
    async def compensate(
        self,
        pre_state: dict | None,
        intent: dict,
        dry_run: bool = False,
    ) -> None:
        """
        Compensating action for rollback (§3.5).

        pre_state is None  → object did not exist before execute(); DELETE it.
        pre_state is a dict → object existed; RESTORE it to pre_state. NEVER delete.

        Must be idempotent and retryable.
        """

    def _log_dry_run(self, operation: str, target: str) -> None:
        import structlog
        structlog.get_logger(__name__).info(
            "step.dry_run", step=self.step_name, operation=operation, target=target
        )
