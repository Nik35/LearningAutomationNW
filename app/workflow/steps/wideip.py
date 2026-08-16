"""
Step 4 of 5 (create): GTM WideIP.

Object order per §3.4: monitor → pool → pool members → WideIP → CNAME
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from app.workflow.steps.base import BaseStep, StepResult

if TYPE_CHECKING:
    from app.clients.f5.gtm import F5GTMClient

log = structlog.get_logger(__name__)


class WideIPStep(BaseStep):
    step_name = "gtm_wideip"
    step_order = 4
    target_system = "f5"
    object_type = "wideip"

    def __init__(self, f5_client: "F5GTMClient") -> None:
        self._client = f5_client

    async def execute(self, intent: dict, dry_run: bool = False) -> StepResult:
        """
        intent keys:
          wideip_type: str         (e.g. "a")
          wip_fqdn: str            (the WideIP name / FQDN)
          wideip_config: dict      (pools ref, poolLbMode, persistence, etc.)
          partition: str
        """
        wideip_type = intent["wideip_type"]
        name = intent["wip_fqdn"]
        config = intent.get("wideip_config", {})
        partition = intent.get("partition", "Common")

        if dry_run:
            self._log_dry_run("ensure_wideip", f"{partition}/{name}")
            pre = await self._client.get_wideip(wideip_type, name, partition)
            return StepResult(action="no_op", pre_state=pre, post_state=pre)

        result = await self._client.ensure_wideip(wideip_type, name, config, partition)
        return StepResult(action=result.action, pre_state=result.pre_state, post_state=result.post_state)

    async def compensate(
        self,
        pre_state: dict | None,
        intent: dict,
        dry_run: bool = False,
    ) -> None:
        wideip_type = intent["wideip_type"]
        name = intent["wip_fqdn"]
        partition = intent.get("partition", "Common")

        if dry_run:
            self._log_dry_run(
                "delete_wideip" if pre_state is None else "restore_wideip",
                f"{partition}/{name}",
            )
            return

        if pre_state is None:
            result = await self._client.delete_wideip(wideip_type, name, partition)
            log.info("step.compensate.wideip_deleted", name=name, action=result.action)
        else:
            result = await self._client.ensure_wideip(wideip_type, name, pre_state, partition)
            log.info("step.compensate.wideip_restored", name=name, action=result.action)
