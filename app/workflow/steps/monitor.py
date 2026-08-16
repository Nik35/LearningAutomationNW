"""
Step 1 of 4 (create) / Step 4 of 4 (delete): GTM Monitor.

Object order per §3.4: monitor → pool → pool members → WideIP → CNAME
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from app.workflow.steps.base import BaseStep, StepResult

if TYPE_CHECKING:
    from app.clients.f5.gtm import F5GTMClient

log = structlog.get_logger(__name__)


class MonitorStep(BaseStep):
    step_name = "gtm_monitor"
    step_order = 1
    target_system = "f5"
    object_type = "monitor"

    def __init__(self, f5_client: "F5GTMClient") -> None:
        self._client = f5_client

    async def execute(self, intent: dict, dry_run: bool = False) -> StepResult:
        """
        Ensure the GTM monitor exists with the desired configuration.

        intent keys expected:
          monitor_type: str   (e.g. "bigip", "http", "https")
          monitor_name: str
          monitor_config: dict  (fields per api-research-findings.md)
          partition: str        (default "Common")
        """
        monitor_type = intent["monitor_type"]
        name = intent["monitor_name"]
        config = intent.get("monitor_config", {})
        partition = intent.get("partition", "Common")

        if dry_run:
            self._log_dry_run("ensure_monitor", f"{partition}/{name}")
            pre = await self._client.get_monitor(monitor_type, name, partition)
            return StepResult(action="no_op", pre_state=pre, post_state=pre)

        result = await self._client.ensure_monitor(monitor_type, name, config, partition)
        return StepResult(
            action=result.action,
            pre_state=result.pre_state,
            post_state=result.post_state,
        )

    async def compensate(
        self,
        pre_state: dict | None,
        intent: dict,
        dry_run: bool = False,
    ) -> None:
        """
        Rollback logic per §3.5:
        - pre_state is None → monitor did not exist before; delete it
        - pre_state is dict → monitor existed; restore prior state
        """
        monitor_type = intent["monitor_type"]
        name = intent["monitor_name"]
        partition = intent.get("partition", "Common")

        if dry_run:
            self._log_dry_run(
                "delete_monitor" if pre_state is None else "restore_monitor",
                f"{partition}/{name}",
            )
            return

        if pre_state is None:
            # Object created by this request — safe to delete
            result = await self._client.delete_monitor(monitor_type, name, partition)
            log.info("step.compensate.monitor_deleted", name=name, action=result.action)
        else:
            # Object pre-existed — restore, never delete
            result = await self._client.ensure_monitor(monitor_type, name, pre_state, partition)
            log.info("step.compensate.monitor_restored", name=name, action=result.action)
