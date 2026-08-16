"""
Operational controls for the GTM automation service (WP-7).

All state is stored in Redis so that toggles propagate to every pod and
worker within one poll interval without requiring a redeploy, satisfying
T-3.6 and T-7.1 through T-7.4.

Redis key schema
----------------
ops:kill_switch                     → "1" | absent
ops:dry_run                         → "1" | absent
ops:delete_count:{window_start}     → integer string; TTL = 2 × window
ops:device_disabled:{device_id}     → "1" | absent
ops:override:{timestamp}            → JSON audit record; TTL = 90 days

All write methods log their action at INFO level using the structured
logger so that every toggle appears in the audit trail.

P-9 (max deletes per window) is accepted as a constructor argument and
NEVER hardcoded here.  The window length (default 1 hour) is also
constructor-configurable.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any

from app.core.logging import get_logger

log = get_logger(__name__)

# Sentinel value stored in Redis for boolean flags.
_TRUE_VALUE = "1"

# How long to keep override audit records (90 days in seconds).
_OVERRIDE_AUDIT_TTL_SECONDS = 90 * 24 * 3600


class OperationalControls:
    """
    Runtime operational controls backed by Redis.

    All methods are async and expect an async-compatible redis client
    (e.g. ``redis.asyncio.Redis``).

    Parameters
    ----------
    redis_client:
        An open async Redis connection/pool.
    settings:
        Object with at least the attributes:
            - ``delete_cap``        : int   — P-9, max deletes per window
            - ``delete_window_seconds`` : int — rolling window length in seconds
                                             (default 3 600 for 1 hour)
        The settings object is intentionally loosely typed so this module
        does not hard-couple to a specific config class.
    """

    # Public key constants — importable by tests or other modules that need
    # to inspect or seed Redis state directly.
    KILL_SWITCH_KEY = "ops:kill_switch"
    DRY_RUN_KEY = "ops:dry_run"
    DELETE_CAP_KEY_TEMPLATE = "ops:delete_count:{window_start}"
    DEVICE_DISABLED_KEY_TEMPLATE = "ops:device_disabled:{device_id}"

    def __init__(self, redis_client: Any, settings: Any) -> None:
        self._redis = redis_client
        # P-9: accepted from settings, never hardcoded.
        self._delete_cap: int = settings.delete_cap
        # Window length for the rolling delete counter.  Defaults to 1 hour.
        self._delete_window_seconds: int = getattr(
            settings, "delete_window_seconds", 3600
        )

    # ------------------------------------------------------------------
    # Kill switch  (T-7.1)
    # ------------------------------------------------------------------

    async def is_kill_switch_active(self) -> bool:
        """Return True if the kill switch is currently engaged."""
        value = await self._redis.get(self.KILL_SWITCH_KEY)
        return value is not None

    async def set_kill_switch(self, active: bool, reason: str) -> None:
        """
        Engage or disengage the kill switch.

        Parameters
        ----------
        active:
            True to engage (halt new F5/Infoblox writes); False to clear.
        reason:
            Mandatory human-readable justification stored in the log.
        """
        if active:
            await self._redis.set(self.KILL_SWITCH_KEY, _TRUE_VALUE)
            log.info(
                "kill_switch_engaged",
                reason=reason,
                active=True,
            )
        else:
            await self._redis.delete(self.KILL_SWITCH_KEY)
            log.info(
                "kill_switch_cleared",
                reason=reason,
                active=False,
            )

    # ------------------------------------------------------------------
    # Dry-run  (T-7.2)
    # ------------------------------------------------------------------

    async def is_dry_run(self) -> bool:
        """Return True if dry-run mode is active."""
        value = await self._redis.get(self.DRY_RUN_KEY)
        return value is not None

    async def set_dry_run(self, active: bool) -> None:
        """
        Enable or disable dry-run mode.

        In dry-run mode the workflow executes fully (all pre-reads,
        comparisons, logging) but no writes are sent to F5 or Infoblox.
        """
        if active:
            await self._redis.set(self.DRY_RUN_KEY, _TRUE_VALUE)
            log.info("dry_run_enabled")
        else:
            await self._redis.delete(self.DRY_RUN_KEY)
            log.info("dry_run_disabled")

    # ------------------------------------------------------------------
    # Destructive cap  (T-7.3)
    # ------------------------------------------------------------------

    def _current_window_start(self) -> int:
        """
        Return the Unix timestamp (seconds) for the start of the current
        rolling window, floored to the window boundary.
        """
        now = int(time.time())
        return now - (now % self._delete_window_seconds)

    def _delete_cap_key(self) -> str:
        window_start = self._current_window_start()
        return self.DELETE_CAP_KEY_TEMPLATE.format(window_start=window_start)

    async def check_delete_cap(self) -> tuple[bool, int]:
        """
        Check whether another delete is permitted in the current window.

        Returns
        -------
        (allowed, current_count)
            ``allowed`` is False when ``current_count >= P-9``.
            ``current_count`` is the number of deletes already committed
            in this window (before the potential new one).
        """
        key = self._delete_cap_key()
        raw = await self._redis.get(key)
        current = int(raw) if raw is not None else 0
        allowed = current < self._delete_cap
        return allowed, current

    async def increment_delete_count(self) -> None:
        """
        Record that one delete has been executed.

        Uses INCR + EXPIREAT so the key auto-expires at the end of the
        window (plus one window of grace to handle clock skew).
        """
        key = self._delete_cap_key()
        pipe = self._redis.pipeline()
        pipe.incr(key)
        # Expire at the start of the *next* window plus one extra window
        # as grace, so we never lose a count that legitimately belongs to
        # this window due to a tight TTL.
        expire_at = self._current_window_start() + 2 * self._delete_window_seconds
        pipe.expireat(key, expire_at)
        await pipe.execute()

    async def override_delete_cap(self, reason: str, actor: str) -> None:
        """
        Record an audited override of the delete cap.

        This method does NOT clear the counter or raise the cap — it
        simply writes a timestamped audit record to Redis so that the
        override can be reviewed later.  The actual delete should
        proceed immediately after the caller confirms the override.

        Parameters
        ----------
        reason:
            Human-readable justification.
        actor:
            Identity of the operator (e.g. username, service account).
        """
        ts = datetime.now(tz=timezone.utc).isoformat()
        audit_key = f"ops:override:{ts}"
        audit_record = json.dumps(
            {
                "type": "delete_cap_override",
                "timestamp": ts,
                "reason": reason,
                "actor": actor,
                "cap": self._delete_cap,
                "window_seconds": self._delete_window_seconds,
            }
        )
        await self._redis.set(
            audit_key, audit_record, ex=_OVERRIDE_AUDIT_TTL_SECONDS
        )
        log.warning(
            "delete_cap_override",
            reason=reason,
            actor=actor,
            cap=self._delete_cap,
        )

    # ------------------------------------------------------------------
    # Per-device disable  (T-7.4)
    # ------------------------------------------------------------------

    def _device_disabled_key(self, device_id: str) -> str:
        return self.DEVICE_DISABLED_KEY_TEMPLATE.format(device_id=device_id)

    async def is_device_disabled(self, device_id: str) -> bool:
        """Return True if the specified device is currently disabled."""
        value = await self._redis.get(self._device_disabled_key(device_id))
        return value is not None

    async def set_device_disabled(
        self, device_id: str, disabled: bool, reason: str
    ) -> None:
        """
        Enable or disable a specific F5 device for new work.

        When disabled, the admission check rejects new requests targeting
        that device with 503 + Retry-After.  In-flight work is unaffected.

        Parameters
        ----------
        device_id:
            The device identifier (e.g. "dc-a-f5-01").
        disabled:
            True to disable the device; False to re-enable.
        reason:
            Mandatory human-readable justification.
        """
        key = self._device_disabled_key(device_id)
        if disabled:
            await self._redis.set(key, _TRUE_VALUE)
            log.info(
                "device_disabled",
                device_id=device_id,
                reason=reason,
            )
        else:
            await self._redis.delete(key)
            log.info(
                "device_enabled",
                device_id=device_id,
                reason=reason,
            )

    # ------------------------------------------------------------------
    # Snapshot for status endpoint  (T-7.5)
    # ------------------------------------------------------------------

    async def get_all_states(self) -> dict[str, Any]:
        """
        Return a complete snapshot of all operational control states.

        Intended for consumption by ``app.ops.status.build_status_snapshot``.
        Does not include per-device metrics (queue depth, breaker) — those
        are assembled by the status module.

        Returns
        -------
        dict with keys:
            kill_switch   : bool
            dry_run       : bool
            delete_window : dict(window_seconds, cap, current_count, allowed)
        """
        kill_switch = await self.is_kill_switch_active()
        dry_run = await self.is_dry_run()
        allowed, current_count = await self.check_delete_cap()

        return {
            "kill_switch": kill_switch,
            "dry_run": dry_run,
            "delete_window": {
                "window_seconds": self._delete_window_seconds,
                "cap": self._delete_cap,
                "current_count": current_count,
                "allowed": allowed,
            },
        }
