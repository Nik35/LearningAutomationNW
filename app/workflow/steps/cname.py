"""
Step 5 of 5 (create): Infoblox CNAME record.

Object order per §3.4: monitor → pool → pool members → WideIP → CNAME

The CNAME is the last step on create and the first step on delete.
On delete: CNAME must be removed BEFORE the WideIP or DNS resolves to nothing.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from app.workflow.steps.base import BaseStep, StepResult

if TYPE_CHECKING:
    from app.clients.infoblox.records import InfobloxClient

log = structlog.get_logger(__name__)


class CNAMEStep(BaseStep):
    step_name = "infoblox_cname"
    step_order = 5
    target_system = "infoblox"
    object_type = "cname"

    def __init__(self, infoblox_client: "InfobloxClient") -> None:
        self._client = infoblox_client

    async def execute(self, intent: dict, dry_run: bool = False) -> StepResult:
        """
        intent keys:
          cname_name: str       (alias FQDN — the record to create)
          cname_canonical: str  (target FQDN — the WideIP FQDN)
          cname_view: str | None
          cname_ttl: int | None
          cname_comment: str | None
        """
        name = intent["cname_name"]
        canonical = intent["cname_canonical"]
        view = intent.get("cname_view")
        ttl = intent.get("cname_ttl")
        comment = intent.get("cname_comment")

        if dry_run:
            self._log_dry_run("ensure_cname", name)
            pre = await self._client.get_cname(name, view)
            return StepResult(action="no_op", pre_state=pre, post_state=pre)

        result = await self._client.ensure_cname(name, canonical, view=view, ttl=ttl, comment=comment)
        return StepResult(action=result.action, pre_state=result.pre_state, post_state=result.post_state)

    async def compensate(
        self,
        pre_state: dict | None,
        intent: dict,
        dry_run: bool = False,
    ) -> None:
        """
        Rollback:
        - pre_state is None → CNAME did not exist; delete what we created
        - pre_state is dict → CNAME existed with different target; restore prior canonical

        Scenario 3 from §7: "WideIP created, CNAME fails" — compensate only removes
        the CNAME if we created it. The WideIP compensation is handled by its own step.
        """
        name = intent["cname_name"]
        canonical = intent["cname_canonical"]
        view = intent.get("cname_view")

        if dry_run:
            self._log_dry_run(
                "delete_cname" if pre_state is None else "restore_cname",
                name,
            )
            return

        if pre_state is None:
            result = await self._client.delete_cname(name, view)
            log.info("step.compensate.cname_deleted", name=name, action=result.action)
        else:
            # Restore previous canonical target
            prior_canonical = pre_state.get("canonical", canonical)
            prior_ttl = pre_state.get("ttl")
            result = await self._client.ensure_cname(name, prior_canonical, view=view, ttl=prior_ttl)
            log.info("step.compensate.cname_restored", name=name, action=result.action)
