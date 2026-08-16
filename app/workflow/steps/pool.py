"""
Steps 2 + 3 of 4 (create): GTM Pool and Pool Members.

Object order per §3.4: monitor → pool → pool members → WideIP → CNAME
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from app.workflow.steps.base import BaseStep, StepResult

if TYPE_CHECKING:
    from app.clients.f5.gtm import F5GTMClient

log = structlog.get_logger(__name__)


class PoolStep(BaseStep):
    step_name = "gtm_pool"
    step_order = 2
    target_system = "f5"
    object_type = "pool"

    def __init__(self, f5_client: "F5GTMClient") -> None:
        self._client = f5_client

    async def execute(self, intent: dict, dry_run: bool = False) -> StepResult:
        """
        intent keys:
          pool_type: str        (e.g. "a")
          pool_name: str
          pool_config: dict     (monitor ref, load_balancing_mode, etc.)
          partition: str
        """
        pool_type = intent["pool_type"]
        name = intent["pool_name"]
        config = intent.get("pool_config", {})
        partition = intent.get("partition", "Common")

        if dry_run:
            self._log_dry_run("ensure_pool", f"{partition}/{name}")
            pre = await self._client.get_pool(pool_type, name, partition)
            return StepResult(action="no_op", pre_state=pre, post_state=pre)

        result = await self._client.ensure_pool(pool_type, name, config, partition)
        return StepResult(action=result.action, pre_state=result.pre_state, post_state=result.post_state)

    async def compensate(
        self,
        pre_state: dict | None,
        intent: dict,
        dry_run: bool = False,
    ) -> None:
        pool_type = intent["pool_type"]
        name = intent["pool_name"]
        partition = intent.get("partition", "Common")

        if dry_run:
            self._log_dry_run(
                "delete_pool" if pre_state is None else "restore_pool",
                f"{partition}/{name}",
            )
            return

        if pre_state is None:
            result = await self._client.delete_pool(pool_type, name, partition)
            log.info("step.compensate.pool_deleted", name=name, action=result.action)
        else:
            result = await self._client.ensure_pool(pool_type, name, pre_state, partition)
            log.info("step.compensate.pool_restored", name=name, action=result.action)


class PoolMembersStep(BaseStep):
    step_name = "gtm_pool_members"
    step_order = 3
    target_system = "f5"
    object_type = "pool_members"

    def __init__(self, f5_client: "F5GTMClient") -> None:
        self._client = f5_client

    async def execute(self, intent: dict, dry_run: bool = False) -> StepResult:
        """
        intent keys:
          pool_type: str
          pool_name: str
          members: list[dict]   (VS references with name, ratio, order fields)
          partition: str
        """
        pool_type = intent["pool_type"]
        pool_name = intent["pool_name"]
        members = intent.get("members", [])
        partition = intent.get("partition", "Common")

        if dry_run:
            self._log_dry_run("ensure_pool_members", f"{partition}/{pool_name}")
            pre = await self._client.get_pool_members(pool_type, pool_name, partition)
            return StepResult(action="no_op", pre_state={"members": pre}, post_state={"members": pre})

        pre_members = await self._client.get_pool_members(pool_type, pool_name, partition)
        result = await self._client.ensure_pool_members(pool_type, pool_name, members, partition)
        return StepResult(
            action=result.action,
            pre_state={"members": pre_members},
            post_state=result.post_state,
        )

    async def compensate(
        self,
        pre_state: dict | None,
        intent: dict,
        dry_run: bool = False,
    ) -> None:
        pool_type = intent["pool_type"]
        pool_name = intent["pool_name"]
        partition = intent.get("partition", "Common")

        if dry_run:
            self._log_dry_run("restore_pool_members", f"{partition}/{pool_name}")
            return

        if pre_state is None or not pre_state.get("members"):
            # No members existed before — delete all members we added
            await self._client.delete_all_pool_members(pool_type, pool_name, partition)
            log.info("step.compensate.pool_members_cleared", pool=pool_name)
        else:
            # Restore prior member list
            prior_members = pre_state["members"]
            result = await self._client.ensure_pool_members(pool_type, pool_name, prior_members, partition)
            log.info("step.compensate.pool_members_restored", pool=pool_name, action=result.action)
