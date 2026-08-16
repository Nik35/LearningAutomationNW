"""
Unit tests for app.ops.controls.OperationalControls.

Uses fakeredis (with Lua support) so no real Redis instance is required.
All P-n values are provided explicitly as test arguments — never hardcoded.

Test coverage
-------------
- Kill switch: set/clear and boolean read-back.
- Dry-run: set/clear and boolean read-back.
- Delete cap:
    - Increments accumulate within the window.
    - Exactly at cap → rejected (boundary: current_count == cap).
    - Below cap → allowed.
    - Override writes an audit record without clearing the counter.
    - New window resets the count (clock-based key rotation).
- Per-device disable/enable: set and read-back for two independent devices.
"""

from __future__ import annotations

import json
import time
from types import SimpleNamespace
from unittest.mock import patch

import fakeredis.aioredis as fakeredis
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_settings(
    *,
    delete_cap: int,
    delete_window_seconds: int = 3600,
) -> SimpleNamespace:
    """Build a minimal settings stub accepted by OperationalControls."""
    return SimpleNamespace(
        delete_cap=delete_cap,
        delete_window_seconds=delete_window_seconds,
    )


@pytest.fixture
async def redis_client():
    """Async fake Redis client, reset between tests."""
    client = fakeredis.FakeRedis()
    yield client
    await client.aclose()


@pytest.fixture
def settings():
    """Default settings with a small cap so tests stay fast."""
    return _make_settings(delete_cap=5, delete_window_seconds=3600)


@pytest.fixture
async def controls(redis_client, settings):
    """Fully initialised OperationalControls against fake Redis."""
    from app.ops.controls import OperationalControls

    return OperationalControls(redis_client, settings)


# ---------------------------------------------------------------------------
# Kill switch
# ---------------------------------------------------------------------------


class TestKillSwitch:
    async def test_inactive_by_default(self, controls):
        assert await controls.is_kill_switch_active() is False

    async def test_set_active(self, controls):
        await controls.set_kill_switch(active=True, reason="load test")
        assert await controls.is_kill_switch_active() is True

    async def test_clear_after_set(self, controls):
        await controls.set_kill_switch(active=True, reason="enabling")
        await controls.set_kill_switch(active=False, reason="disabling")
        assert await controls.is_kill_switch_active() is False

    async def test_set_twice_idempotent(self, controls):
        await controls.set_kill_switch(active=True, reason="first")
        await controls.set_kill_switch(active=True, reason="second")
        assert await controls.is_kill_switch_active() is True

    async def test_clear_when_not_set_is_noop(self, controls):
        await controls.set_kill_switch(active=False, reason="clearing nothing")
        assert await controls.is_kill_switch_active() is False

    async def test_round_trip(self, controls):
        """Engage → disengage → engage verifies independent state."""
        assert await controls.is_kill_switch_active() is False
        await controls.set_kill_switch(active=True, reason="on")
        assert await controls.is_kill_switch_active() is True
        await controls.set_kill_switch(active=False, reason="off")
        assert await controls.is_kill_switch_active() is False
        await controls.set_kill_switch(active=True, reason="on again")
        assert await controls.is_kill_switch_active() is True


# ---------------------------------------------------------------------------
# Dry-run
# ---------------------------------------------------------------------------


class TestDryRun:
    async def test_inactive_by_default(self, controls):
        assert await controls.is_dry_run() is False

    async def test_enable(self, controls):
        await controls.set_dry_run(active=True)
        assert await controls.is_dry_run() is True

    async def test_disable_after_enable(self, controls):
        await controls.set_dry_run(active=True)
        await controls.set_dry_run(active=False)
        assert await controls.is_dry_run() is False

    async def test_enable_twice_idempotent(self, controls):
        await controls.set_dry_run(active=True)
        await controls.set_dry_run(active=True)
        assert await controls.is_dry_run() is True

    async def test_round_trip(self, controls):
        assert await controls.is_dry_run() is False
        await controls.set_dry_run(active=True)
        assert await controls.is_dry_run() is True
        await controls.set_dry_run(active=False)
        assert await controls.is_dry_run() is False

    async def test_dry_run_independent_of_kill_switch(self, controls):
        """Kill switch and dry-run flags do not interfere with each other."""
        await controls.set_kill_switch(active=True, reason="test")
        await controls.set_dry_run(active=True)
        assert await controls.is_kill_switch_active() is True
        assert await controls.is_dry_run() is True
        await controls.set_kill_switch(active=False, reason="test")
        assert await controls.is_kill_switch_active() is False
        assert await controls.is_dry_run() is True


# ---------------------------------------------------------------------------
# Delete cap  (P-9)
# ---------------------------------------------------------------------------


class TestDeleteCap:
    async def test_allowed_when_no_deletes(self, controls):
        allowed, count = await controls.check_delete_cap()
        assert allowed is True
        assert count == 0

    async def test_allowed_below_cap(self, controls):
        """Incrementing to cap - 1 still allows."""
        cap = controls._delete_cap  # 5 in fixture
        for _ in range(cap - 1):
            await controls.increment_delete_count()
        allowed, count = await controls.check_delete_cap()
        assert allowed is True
        assert count == cap - 1

    async def test_rejected_at_cap(self, controls):
        """Exactly at cap → not allowed."""
        cap = controls._delete_cap  # 5 in fixture
        for _ in range(cap):
            await controls.increment_delete_count()
        allowed, count = await controls.check_delete_cap()
        assert allowed is False
        assert count == cap

    async def test_rejected_above_cap(self, controls):
        """Above cap (edge case: two concurrent increments near cap)."""
        cap = controls._delete_cap
        for _ in range(cap + 2):
            await controls.increment_delete_count()
        allowed, count = await controls.check_delete_cap()
        assert allowed is False
        assert count > cap

    async def test_increment_accumulates(self, controls):
        """Each increment is reflected in check_delete_cap."""
        for expected in range(1, 4):
            await controls.increment_delete_count()
            _, count = await controls.check_delete_cap()
            assert count == expected

    async def test_cap_is_parameterised(self, redis_client):
        """P-9 = 1 means first delete is allowed, second is not."""
        from app.ops.controls import OperationalControls

        s = _make_settings(delete_cap=1)
        c = OperationalControls(redis_client, s)

        allowed, count = await c.check_delete_cap()
        assert allowed is True and count == 0

        await c.increment_delete_count()

        allowed, count = await c.check_delete_cap()
        assert allowed is False and count == 1

    async def test_new_window_resets_count(self, redis_client):
        """
        Simulating a clock advance past the window boundary produces a
        fresh counter.  We do this by patching time.time() to return a
        value 3 600 + 1 seconds later than the first batch of increments.
        """
        from app.ops.controls import OperationalControls

        s = _make_settings(delete_cap=3, delete_window_seconds=3600)
        c = OperationalControls(redis_client, s)

        # Fill the cap in window 1.
        for _ in range(3):
            await c.increment_delete_count()
        allowed_w1, count_w1 = await c.check_delete_cap()
        assert allowed_w1 is False and count_w1 == 3

        # Advance clock past the window boundary.
        future = time.time() + 3601
        with patch("app.ops.controls.time.time", return_value=future):
            allowed_w2, count_w2 = await c.check_delete_cap()
        assert allowed_w2 is True and count_w2 == 0

    async def test_override_writes_audit_record(self, controls, redis_client):
        """override_delete_cap writes an audit key without altering the counter."""
        cap = controls._delete_cap
        for _ in range(cap):
            await controls.increment_delete_count()

        allowed_before, _ = await controls.check_delete_cap()
        assert allowed_before is False

        await controls.override_delete_cap(reason="emergency deploy", actor="ops-bot")

        # Counter must still be at cap (override does not clear it).
        allowed_after, count_after = await controls.check_delete_cap()
        assert allowed_after is False
        assert count_after == cap

        # An audit key must exist and contain the expected fields.
        keys = [k async for k in redis_client.scan_iter("ops:override:*")]
        assert len(keys) == 1, "Expected exactly one audit record"

        raw = await redis_client.get(keys[0])
        record = json.loads(raw)
        assert record["type"] == "delete_cap_override"
        assert record["reason"] == "emergency deploy"
        assert record["actor"] == "ops-bot"
        assert record["cap"] == cap

    async def test_override_leaves_counter_unchanged(self, controls):
        """Multiple overrides accumulate audit keys; counter is unaffected."""
        await controls.increment_delete_count()
        await controls.override_delete_cap(reason="r1", actor="alice")
        await controls.override_delete_cap(reason="r2", actor="bob")
        _, count = await controls.check_delete_cap()
        assert count == 1


# ---------------------------------------------------------------------------
# Per-device disable / enable
# ---------------------------------------------------------------------------


class TestDeviceDisable:
    async def test_enabled_by_default(self, controls):
        assert await controls.is_device_disabled("dc-a-f5-01") is False

    async def test_disable_device(self, controls):
        await controls.set_device_disabled("dc-a-f5-01", disabled=True, reason="maintenance")
        assert await controls.is_device_disabled("dc-a-f5-01") is True

    async def test_enable_after_disable(self, controls):
        await controls.set_device_disabled("dc-a-f5-01", disabled=True, reason="maint")
        await controls.set_device_disabled("dc-a-f5-01", disabled=False, reason="done")
        assert await controls.is_device_disabled("dc-a-f5-01") is False

    async def test_two_devices_independent(self, controls):
        """Disabling one device must not affect the other."""
        await controls.set_device_disabled("dc-a-f5-01", disabled=True, reason="maint")
        assert await controls.is_device_disabled("dc-a-f5-01") is True
        assert await controls.is_device_disabled("dc-b-f5-01") is False

    async def test_disable_all_devices(self, controls):
        for dev in ["dc-a-f5-01", "dc-a-f5-02", "dc-b-f5-01", "dc-b-f5-02"]:
            await controls.set_device_disabled(dev, disabled=True, reason="full maintenance")
        for dev in ["dc-a-f5-01", "dc-a-f5-02", "dc-b-f5-01", "dc-b-f5-02"]:
            assert await controls.is_device_disabled(dev) is True

    async def test_reenable_all_devices(self, controls):
        devices = ["dc-a-f5-01", "dc-a-f5-02", "dc-b-f5-01", "dc-b-f5-02"]
        for dev in devices:
            await controls.set_device_disabled(dev, disabled=True, reason="maint")
        for dev in devices:
            await controls.set_device_disabled(dev, disabled=False, reason="done")
        for dev in devices:
            assert await controls.is_device_disabled(dev) is False

    async def test_disable_idempotent(self, controls):
        await controls.set_device_disabled("dc-a-f5-01", disabled=True, reason="maint")
        await controls.set_device_disabled("dc-a-f5-01", disabled=True, reason="maint again")
        assert await controls.is_device_disabled("dc-a-f5-01") is True

    async def test_device_id_in_key(self, redis_client, settings):
        """
        Verify that the Redis key contains the device_id so different
        devices do not collide.
        """
        from app.ops.controls import OperationalControls

        c = OperationalControls(redis_client, settings)
        await c.set_device_disabled("dc-a-f5-01", disabled=True, reason="test")

        # The key for dc-a-f5-01 should exist.
        key_a = c.DEVICE_DISABLED_KEY_TEMPLATE.format(device_id="dc-a-f5-01")
        key_b = c.DEVICE_DISABLED_KEY_TEMPLATE.format(device_id="dc-b-f5-01")

        assert await redis_client.exists(key_a) == 1
        assert await redis_client.exists(key_b) == 0


# ---------------------------------------------------------------------------
# get_all_states snapshot
# ---------------------------------------------------------------------------


class TestGetAllStates:
    async def test_default_snapshot(self, controls):
        state = await controls.get_all_states()
        assert state["kill_switch"] is False
        assert state["dry_run"] is False
        assert state["delete_window"]["current_count"] == 0
        assert state["delete_window"]["allowed"] is True

    async def test_snapshot_reflects_changes(self, controls):
        await controls.set_kill_switch(active=True, reason="test")
        await controls.set_dry_run(active=True)
        await controls.increment_delete_count()

        state = await controls.get_all_states()
        assert state["kill_switch"] is True
        assert state["dry_run"] is True
        assert state["delete_window"]["current_count"] == 1
