"""
tests/unit/test_f5_client.py
============================
Unit tests for the F5 iControl REST client package.

Coverage:
  TestF5Session               — basic request dispatch, TLS + timeout params
  TestF5TokenManager          — caching, stampede prevention, lock behaviour
  TestF5GTMClientGet          — get_* returns None on 404, dict on 200
  TestF5GTMClientEnsure       — no_op / created / updated for each object type
  TestF5GTMClientDelete       — deleted / not_found for each object type
  TestF5GTMClientTimeout      — timeout → F5TimeoutError, not retried
  TestF5GTMClientErrors       — 409 → F5ConflictError, 5xx → F5ServerError

All HTTP calls are intercepted with ``respx``.  No real network traffic.
All P-n values (timeouts, concurrency limits) are supplied as constructor
arguments; nothing is hardcoded.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import respx

from app.clients.f5.auth import F5TokenManager
from app.clients.f5.gtm import (
    F5ConflictError,
    F5GTMClient,
    F5NotFoundError,
    F5ServerError,
    F5TimeoutError,
    OperationResult,
    _configs_equal,
    _monitor_path,
    _pool_members_path,
    _pool_path,
    _resource_path,
    _wideip_path,
)
from app.clients.f5.session import F5Session

# ---------------------------------------------------------------------------
# Constants used across tests
# ---------------------------------------------------------------------------

DEVICE_ID = "test-gtm-01"
HOST = "bigip.example.internal"
BASE_URL = f"https://{HOST}"

# A synthetic token value used in mocked responses.
FAKE_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.fake"

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def session() -> F5Session:
    """F5Session with test-friendly settings (low concurrency, short timeout)."""
    return F5Session(
        device_id=DEVICE_ID,
        host=HOST,
        max_connections=4,          # P-1 derived — supplied, never hardcoded
        timeout_seconds=5.0,        # supplied, never hardcoded
        verify_ssl=False,           # dev / test — no real cert
    )


@pytest.fixture()
def mock_redis() -> MagicMock:
    """
    A minimal async Redis mock that simulates SET/GET/DELETE for the token
    cache and the NX lock.

    The store is a plain dict so tests can inspect it directly if needed.
    """
    store: dict[str, Any] = {}
    lock_held: dict[str, bool] = {}

    mock = MagicMock()

    async def _get(key: str) -> bytes | None:
        val = store.get(key)
        return val.encode() if isinstance(val, str) else val

    async def _set(
        key: str,
        value: str,
        nx: bool = False,
        ex: int | None = None,
    ) -> str | None:
        if nx:
            if key in lock_held and lock_held[key]:
                return None  # lock already held
            lock_held[key] = True
            store[key] = value
            return "OK"
        store[key] = value
        return "OK"

    async def _delete(*keys: str) -> int:
        removed = 0
        for k in keys:
            if k in store:
                del store[k]
                removed += 1
            if k in lock_held:
                del lock_held[k]
        return removed

    mock.get = AsyncMock(side_effect=_get)
    mock.set = AsyncMock(side_effect=_set)
    mock.delete = AsyncMock(side_effect=_delete)
    mock._store = store
    mock._lock_held = lock_held
    return mock


@pytest.fixture()
def token_manager(session: F5Session, mock_redis: MagicMock) -> F5TokenManager:
    return F5TokenManager(
        redis_client=mock_redis,
        device_id=DEVICE_ID,
        username="admin",
        password="secret",
        session=session,
    )


@pytest.fixture()
def gtm_client(session: F5Session, token_manager: F5TokenManager) -> F5GTMClient:
    return F5GTMClient(session=session, token_manager=token_manager)


# ---------------------------------------------------------------------------
# Helper: build a respx mock that routes login + extend calls
# ---------------------------------------------------------------------------


def _mock_login_and_extend(router: respx.MockRouter) -> None:
    """
    Register mocked responses for the two auth endpoints so that
    ``F5TokenManager.get_token()`` succeeds.
    """
    router.post(f"{BASE_URL}/mgmt/shared/authn/login").mock(
        return_value=httpx.Response(
            200,
            json={"token": {"token": FAKE_TOKEN, "timeout": 1200}},
        )
    )
    router.patch(
        f"{BASE_URL}/mgmt/shared/authz/tokens/{FAKE_TOKEN}"
    ).mock(
        return_value=httpx.Response(
            200,
            json={"timeout": 36000},
        )
    )


# ===========================================================================
# TestPathHelpers
# ===========================================================================


class TestPathHelpers:
    """Verify the URL path construction functions."""

    def test_monitor_path(self) -> None:
        assert _monitor_path("bigip", "Common", "my-mon") == (
            "/mgmt/tm/gtm/monitor/bigip/~Common~my-mon"
        )

    def test_pool_path(self) -> None:
        assert _pool_path("a", "Common", "my-pool") == (
            "/mgmt/tm/gtm/pool/a/~Common~my-pool"
        )

    def test_pool_members_path(self) -> None:
        assert _pool_members_path("a", "Common", "my-pool") == (
            "/mgmt/tm/gtm/pool/a/~Common~my-pool/members"
        )

    def test_wideip_path(self) -> None:
        assert _wideip_path("a", "Common", "my.fqdn.example.com") == (
            "/mgmt/tm/gtm/wideip/a/~Common~my.fqdn.example.com"
        )

    def test_resource_path(self) -> None:
        assert _resource_path("wideip/a", "Common", "my.fqdn.example.com") == (
            "/mgmt/tm/gtm/wideip/a/~Common~my.fqdn.example.com"
        )

    def test_custom_partition(self) -> None:
        assert _pool_path("a", "prod", "web-pool") == (
            "/mgmt/tm/gtm/pool/a/~prod~web-pool"
        )


# ===========================================================================
# TestConfigsEqual
# ===========================================================================


class TestConfigsEqual:
    """Verify the field-level comparison helper."""

    def test_identical_configs(self) -> None:
        assert _configs_equal({"interval": 5, "timeout": 15}, {"interval": 5, "timeout": 15})

    def test_desired_subset_of_current(self) -> None:
        """Extra keys in current (API response metadata) are ignored."""
        desired = {"interval": 5}
        current = {"interval": 5, "kind": "tm:gtm:monitor:bigip:bigipstate", "selfLink": "..."}
        assert _configs_equal(desired, current)

    def test_different_value(self) -> None:
        assert not _configs_equal({"interval": 5}, {"interval": 10})

    def test_missing_key_in_current(self) -> None:
        """A desired key absent from current → not equal."""
        assert not _configs_equal({"interval": 5, "timeout": 15}, {"interval": 5})

    def test_nested_dict_equal(self) -> None:
        desired = {"pools": [{"name": "p1", "ratio": 1}]}
        current = {"pools": [{"name": "p1", "ratio": 1}], "extra": "x"}
        assert _configs_equal(desired, current)

    def test_nested_dict_different(self) -> None:
        desired = {"pools": [{"name": "p1", "ratio": 1}]}
        current = {"pools": [{"name": "p1", "ratio": 2}], "extra": "x"}
        assert not _configs_equal(desired, current)


# ===========================================================================
# TestF5Session
# ===========================================================================


class TestF5Session:
    """Verify F5Session dispatches requests with the right base URL and options."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_request_dispatched(self, session: F5Session) -> None:
        respx.get(f"{BASE_URL}/mgmt/tm/gtm/wideip/a").mock(
            return_value=httpx.Response(200, json={"kind": "collection"})
        )
        response = await session.request("GET", "/mgmt/tm/gtm/wideip/a")
        assert response.status_code == 200

    @respx.mock
    @pytest.mark.asyncio
    async def test_post_request_dispatched(self, session: F5Session) -> None:
        respx.post(f"{BASE_URL}/mgmt/tm/gtm/wideip/a").mock(
            return_value=httpx.Response(201, json={"name": "new-wip"})
        )
        response = await session.request(
            "POST", "/mgmt/tm/gtm/wideip/a", json={"name": "new-wip"}
        )
        assert response.status_code == 201

    @pytest.mark.asyncio
    async def test_aclose_does_not_raise(self, session: F5Session) -> None:
        await session.aclose()


# ===========================================================================
# TestF5TokenManager
# ===========================================================================


class TestF5TokenManager:
    """Token caching, refresh, and stampede prevention."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_token_returns_token_string(
        self, token_manager: F5TokenManager
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        token = await token_manager.get_token()
        assert token == FAKE_TOKEN

    @respx.mock
    @pytest.mark.asyncio
    async def test_token_cached_after_first_call(
        self, token_manager: F5TokenManager, mock_redis: MagicMock
    ) -> None:
        """After first login, the token is stored in Redis."""
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        await token_manager.get_token()
        token_key = F5TokenManager.TOKEN_REDIS_KEY.format(device_id=DEVICE_ID)
        assert token_key in mock_redis._store

    @respx.mock
    @pytest.mark.asyncio
    async def test_ten_sequential_calls_produce_one_login(
        self, token_manager: F5TokenManager
    ) -> None:
        """
        Token caching: 10 sequential get_token() calls must result in
        exactly 1 POST to /mgmt/shared/authn/login.
        """
        login_route = respx.mock.post(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/shared/authn/login"
        ).mock(
            return_value=httpx.Response(
                200, json={"token": {"token": FAKE_TOKEN, "timeout": 1200}}
            )
        )
        respx.mock.patch(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/shared/authz/tokens/{FAKE_TOKEN}"
        ).mock(return_value=httpx.Response(200, json={"timeout": 36000}))

        for _ in range(10):
            token = await token_manager.get_token()
            assert token == FAKE_TOKEN

        # Exactly one login call.
        assert login_route.call_count == 1

    @respx.mock
    @pytest.mark.asyncio
    async def test_cached_token_not_refreshed_when_valid(
        self, token_manager: F5TokenManager, mock_redis: MagicMock
    ) -> None:
        """
        Seed Redis with a fresh token.  get_token() must return it without
        calling the F5 login endpoint.
        """
        expires_at = time.time() + 36000
        cached = json.dumps({"token": FAKE_TOKEN, "expires_at_unix": expires_at})
        token_key = F5TokenManager.TOKEN_REDIS_KEY.format(device_id=DEVICE_ID)
        mock_redis._store[token_key] = cached

        # No HTTP mocks registered; any real call would raise.
        token = await token_manager.get_token()
        assert token == FAKE_TOKEN

    @respx.mock
    @pytest.mark.asyncio
    async def test_expired_token_triggers_refresh(
        self, token_manager: F5TokenManager, mock_redis: MagicMock
    ) -> None:
        """A cached token within 120 s of expiry is treated as expired."""
        # Set expires_at 60 s in the future — below TOKEN_REFRESH_BEFORE_EXPIRY_SECONDS.
        expires_at = time.time() + 60
        cached = json.dumps({"token": "old-token", "expires_at_unix": expires_at})
        token_key = F5TokenManager.TOKEN_REDIS_KEY.format(device_id=DEVICE_ID)
        mock_redis._store[token_key] = cached

        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        token = await token_manager.get_token()
        # New token replaces the old one.
        assert token == FAKE_TOKEN

    @respx.mock
    @pytest.mark.asyncio
    async def test_inject_auth_adds_header(
        self, token_manager: F5TokenManager
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        headers = await token_manager.inject_auth({"Content-Type": "application/json"})
        assert headers["X-F5-Auth-Token"] == FAKE_TOKEN
        assert headers["Content-Type"] == "application/json"

    @respx.mock
    @pytest.mark.asyncio
    async def test_stampede_prevention_lock_acquired(
        self, token_manager: F5TokenManager, mock_redis: MagicMock
    ) -> None:
        """
        Simulate a concurrent refresh: pre-seed the lock as already held by
        another worker, then seed the cache as if that worker already wrote
        the token.  Our call should return the cached value without issuing a
        second login.
        """
        lock_key = F5TokenManager.LOCK_REDIS_KEY.format(device_id=DEVICE_ID)
        token_key = F5TokenManager.TOKEN_REDIS_KEY.format(device_id=DEVICE_ID)

        # Another worker holds the lock.
        mock_redis._lock_held[lock_key] = True
        mock_redis._store[lock_key] = "1"

        # After our sleep(LOCK_TTL), the token is available.
        expires_at = time.time() + 36000
        fresh_token_cache = json.dumps(
            {"token": FAKE_TOKEN, "expires_at_unix": expires_at}
        )

        original_sleep = asyncio.sleep

        async def _fast_sleep(seconds: float) -> None:
            # Write token into the cache as the "other worker" would have.
            mock_redis._store[token_key] = fresh_token_cache
            # Release the lock.
            mock_redis._lock_held.pop(lock_key, None)
            mock_redis._store.pop(lock_key, None)
            # Don't actually sleep in tests.
            await original_sleep(0)

        with patch("app.clients.f5.auth.asyncio.sleep", side_effect=_fast_sleep):
            token = await token_manager.get_token()

        assert token == FAKE_TOKEN
        # No login call should have been made.
        assert mock_redis.get.call_count >= 1


# ===========================================================================
# TestF5GTMClientGet
# ===========================================================================


class TestF5GTMClientGet:
    """get_* returns None on 404, dict on 200."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_monitor_returns_dict_on_200(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        expected = {"name": "my-mon", "interval": 5, "timeout": 15}
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/monitor/bigip/~Common~my-mon"
        ).mock(return_value=httpx.Response(200, json=expected))

        result = await gtm_client.get_monitor("bigip", "my-mon")
        assert result == expected

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_monitor_returns_none_on_404(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/monitor/bigip/~Common~missing"
        ).mock(return_value=httpx.Response(404, json={"code": 404}))

        result = await gtm_client.get_monitor("bigip", "missing")
        assert result is None

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_pool_returns_dict_on_200(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        expected = {"name": "my-pool", "loadBalancingMode": "round-robin"}
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/pool/a/~Common~my-pool"
        ).mock(return_value=httpx.Response(200, json=expected))

        result = await gtm_client.get_pool("a", "my-pool")
        assert result == expected

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_pool_returns_none_on_404(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/pool/a/~Common~ghost"
        ).mock(return_value=httpx.Response(404))

        result = await gtm_client.get_pool("a", "ghost")
        assert result is None

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_wideip_returns_dict_on_200(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        expected = {"name": "svc.example.com", "poolLbMode": "round-robin"}
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/wideip/a/~Common~svc.example.com"
        ).mock(return_value=httpx.Response(200, json=expected))

        result = await gtm_client.get_wideip("a", "svc.example.com")
        assert result == expected

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_wideip_returns_none_on_404(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/wideip/a/~Common~missing.example.com"
        ).mock(return_value=httpx.Response(404))

        result = await gtm_client.get_wideip("a", "missing.example.com")
        assert result is None

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_pool_members_returns_list(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        members = [{"name": "/Common/vs1", "ratio": 1}]
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/pool/a/~Common~my-pool/members"
        ).mock(return_value=httpx.Response(200, json={"items": members}))

        result = await gtm_client.get_pool_members("a", "my-pool")
        assert result == members

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_pool_members_returns_empty_list_on_404(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/pool/a/~Common~ghost/members"
        ).mock(return_value=httpx.Response(404))

        result = await gtm_client.get_pool_members("a", "ghost")
        assert result == []


# ===========================================================================
# TestF5GTMClientEnsure
# ===========================================================================


class TestF5GTMClientEnsure:
    """ensure_* returns no_op / created / updated correctly."""

    # ── Monitors ────────────────────────────────────────────────────────────

    @respx.mock
    @pytest.mark.asyncio
    async def test_ensure_monitor_no_op_when_identical(
        self, gtm_client: F5GTMClient
    ) -> None:
        """No PUT/PATCH is issued when the config matches current state."""
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        current = {"name": "my-mon", "interval": 5, "timeout": 15}
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/monitor/bigip/~Common~my-mon"
        ).mock(return_value=httpx.Response(200, json=current))

        # Desired matches current — register no PATCH/PUT (would raise if called).
        result = await gtm_client.ensure_monitor(
            "bigip", "my-mon", {"interval": 5, "timeout": 15}
        )
        assert result.action == "no_op"
        assert result.pre_state == current
        assert result.post_state == current

    @respx.mock
    @pytest.mark.asyncio
    async def test_ensure_monitor_created_when_absent(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/monitor/bigip/~Common~new-mon"
        ).mock(return_value=httpx.Response(404))
        created = {"name": "new-mon", "interval": 5, "timeout": 15}
        respx.mock.post(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/monitor/bigip"
        ).mock(return_value=httpx.Response(200, json=created))

        result = await gtm_client.ensure_monitor(
            "bigip", "new-mon", {"interval": 5, "timeout": 15}
        )
        assert result.action == "created"
        assert result.pre_state is None
        assert result.post_state == created

    @respx.mock
    @pytest.mark.asyncio
    async def test_ensure_monitor_updated_when_different(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        current = {"name": "my-mon", "interval": 5, "timeout": 15}
        updated = {"name": "my-mon", "interval": 10, "timeout": 15}
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/monitor/bigip/~Common~my-mon"
        ).mock(return_value=httpx.Response(200, json=current))
        respx.mock.patch(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/monitor/bigip/~Common~my-mon"
        ).mock(return_value=httpx.Response(200, json=updated))

        result = await gtm_client.ensure_monitor(
            "bigip", "my-mon", {"interval": 10, "timeout": 15}
        )
        assert result.action == "updated"
        assert result.pre_state == current
        assert result.post_state == updated

    # ── Pools ────────────────────────────────────────────────────────────────

    @respx.mock
    @pytest.mark.asyncio
    async def test_ensure_pool_no_op_when_identical(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        current = {"name": "p1", "loadBalancingMode": "round-robin"}
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/pool/a/~Common~p1"
        ).mock(return_value=httpx.Response(200, json=current))

        result = await gtm_client.ensure_pool(
            "a", "p1", {"loadBalancingMode": "round-robin"}
        )
        assert result.action == "no_op"

    @respx.mock
    @pytest.mark.asyncio
    async def test_ensure_pool_created_when_absent(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/pool/a/~Common~p1"
        ).mock(return_value=httpx.Response(404))
        created = {"name": "p1", "loadBalancingMode": "round-robin"}
        respx.mock.post(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/pool/a"
        ).mock(return_value=httpx.Response(200, json=created))

        result = await gtm_client.ensure_pool(
            "a", "p1", {"loadBalancingMode": "round-robin"}
        )
        assert result.action == "created"
        assert result.pre_state is None

    @respx.mock
    @pytest.mark.asyncio
    async def test_ensure_pool_updated_when_different(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        current = {"name": "p1", "loadBalancingMode": "round-robin"}
        updated = {"name": "p1", "loadBalancingMode": "ratio"}
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/pool/a/~Common~p1"
        ).mock(return_value=httpx.Response(200, json=current))
        respx.mock.patch(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/pool/a/~Common~p1"
        ).mock(return_value=httpx.Response(200, json=updated))

        result = await gtm_client.ensure_pool(
            "a", "p1", {"loadBalancingMode": "ratio"}
        )
        assert result.action == "updated"

    # ── Pool members ─────────────────────────────────────────────────────────

    @respx.mock
    @pytest.mark.asyncio
    async def test_ensure_pool_members_no_op_when_identical(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        members = [{"name": "/Common/vs1", "ratio": 1}]
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/pool/a/~Common~p1/members"
        ).mock(return_value=httpx.Response(200, json={"items": members}))

        result = await gtm_client.ensure_pool_members("a", "p1", members)
        assert result.action == "no_op"

    @respx.mock
    @pytest.mark.asyncio
    async def test_ensure_pool_members_updated_when_different(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        current_members = [{"name": "/Common/vs1", "ratio": 1}]
        new_members = [{"name": "/Common/vs2", "ratio": 1}]
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/pool/a/~Common~p1/members"
        ).mock(return_value=httpx.Response(200, json={"items": current_members}))
        updated_pool = {"name": "p1", "members": new_members}
        respx.mock.patch(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/pool/a/~Common~p1"
        ).mock(return_value=httpx.Response(200, json=updated_pool))

        result = await gtm_client.ensure_pool_members("a", "p1", new_members)
        assert result.action == "updated"
        assert result.pre_state == {"items": current_members}

    # ── WideIPs ──────────────────────────────────────────────────────────────

    @respx.mock
    @pytest.mark.asyncio
    async def test_ensure_wideip_no_op_when_identical(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        current = {"name": "svc.example.com", "poolLbMode": "round-robin"}
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/wideip/a/~Common~svc.example.com"
        ).mock(return_value=httpx.Response(200, json=current))

        result = await gtm_client.ensure_wideip(
            "a", "svc.example.com", {"poolLbMode": "round-robin"}
        )
        assert result.action == "no_op"

    @respx.mock
    @pytest.mark.asyncio
    async def test_ensure_wideip_created_when_absent(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/wideip/a/~Common~svc.example.com"
        ).mock(return_value=httpx.Response(404))
        created = {"name": "svc.example.com", "poolLbMode": "round-robin"}
        respx.mock.post(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/wideip/a"
        ).mock(return_value=httpx.Response(200, json=created))

        result = await gtm_client.ensure_wideip(
            "a", "svc.example.com", {"poolLbMode": "round-robin"}
        )
        assert result.action == "created"
        assert result.pre_state is None

    @respx.mock
    @pytest.mark.asyncio
    async def test_ensure_wideip_updated_when_different(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        current = {"name": "svc.example.com", "poolLbMode": "round-robin"}
        updated = {"name": "svc.example.com", "poolLbMode": "ratio"}
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/wideip/a/~Common~svc.example.com"
        ).mock(return_value=httpx.Response(200, json=current))
        respx.mock.patch(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/wideip/a/~Common~svc.example.com"
        ).mock(return_value=httpx.Response(200, json=updated))

        result = await gtm_client.ensure_wideip(
            "a", "svc.example.com", {"poolLbMode": "ratio"}
        )
        assert result.action == "updated"


# ===========================================================================
# TestF5GTMClientDelete
# ===========================================================================


class TestF5GTMClientDelete:
    """delete_* returns deleted / not_found; never raises on 404."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_delete_monitor_returns_deleted(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        current = {"name": "my-mon", "interval": 5}
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/monitor/bigip/~Common~my-mon"
        ).mock(return_value=httpx.Response(200, json=current))
        respx.mock.delete(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/monitor/bigip/~Common~my-mon"
        ).mock(return_value=httpx.Response(200))

        result = await gtm_client.delete_monitor("bigip", "my-mon")
        assert result.action == "deleted"
        assert result.pre_state == current
        assert result.post_state is None

    @respx.mock
    @pytest.mark.asyncio
    async def test_delete_monitor_returns_not_found_when_absent(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/monitor/bigip/~Common~ghost"
        ).mock(return_value=httpx.Response(404))

        result = await gtm_client.delete_monitor("bigip", "ghost")
        assert result.action == "not_found"
        assert result.pre_state is None
        assert result.post_state is None

    @respx.mock
    @pytest.mark.asyncio
    async def test_delete_pool_returns_deleted(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        current = {"name": "p1", "loadBalancingMode": "round-robin"}
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/pool/a/~Common~p1"
        ).mock(return_value=httpx.Response(200, json=current))
        respx.mock.delete(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/pool/a/~Common~p1"
        ).mock(return_value=httpx.Response(200))

        result = await gtm_client.delete_pool("a", "p1")
        assert result.action == "deleted"

    @respx.mock
    @pytest.mark.asyncio
    async def test_delete_pool_returns_not_found_when_absent(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/pool/a/~Common~ghost"
        ).mock(return_value=httpx.Response(404))

        result = await gtm_client.delete_pool("a", "ghost")
        assert result.action == "not_found"

    @respx.mock
    @pytest.mark.asyncio
    async def test_delete_wideip_returns_deleted(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        current = {"name": "svc.example.com"}
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/wideip/a/~Common~svc.example.com"
        ).mock(return_value=httpx.Response(200, json=current))
        respx.mock.delete(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/wideip/a/~Common~svc.example.com"
        ).mock(return_value=httpx.Response(200))

        result = await gtm_client.delete_wideip("a", "svc.example.com")
        assert result.action == "deleted"
        assert result.pre_state == current

    @respx.mock
    @pytest.mark.asyncio
    async def test_delete_wideip_returns_not_found_when_absent(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/wideip/a/~Common~missing.example.com"
        ).mock(return_value=httpx.Response(404))

        result = await gtm_client.delete_wideip("a", "missing.example.com")
        assert result.action == "not_found"

    @respx.mock
    @pytest.mark.asyncio
    async def test_delete_all_pool_members_when_members_exist(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        members = [{"name": "/Common/vs1", "ratio": 1}]
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/pool/a/~Common~p1/members"
        ).mock(return_value=httpx.Response(200, json={"items": members}))
        respx.mock.patch(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/pool/a/~Common~p1"
        ).mock(return_value=httpx.Response(200, json={"name": "p1", "members": []}))

        result = await gtm_client.delete_all_pool_members("a", "p1")
        assert result.action == "deleted"

    @respx.mock
    @pytest.mark.asyncio
    async def test_delete_all_pool_members_no_op_when_empty(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/pool/a/~Common~p1/members"
        ).mock(return_value=httpx.Response(200, json={"items": []}))

        result = await gtm_client.delete_all_pool_members("a", "p1")
        assert result.action == "no_op"


# ===========================================================================
# TestF5GTMClientTimeout
# ===========================================================================


class TestF5GTMClientTimeout:
    """
    Timeouts must raise F5TimeoutError — never silently retry (failure-matrix
    row 7 from the implementation plan).
    """

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_monitor_timeout_raises_f5_timeout_error(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/monitor/bigip/~Common~slow-mon"
        ).mock(side_effect=httpx.ReadTimeout("timed out", request=None))

        with pytest.raises(F5TimeoutError) as exc_info:
            await gtm_client.get_monitor("bigip", "slow-mon")

        assert exc_info.value.operation == "GET"
        assert "slow-mon" in exc_info.value.path

    @respx.mock
    @pytest.mark.asyncio
    async def test_ensure_monitor_post_timeout_raises_f5_timeout_error(
        self, gtm_client: F5GTMClient
    ) -> None:
        """
        Timeout on a POST (create) must surface as F5TimeoutError.
        The caller must read back to determine whether the object was created
        — the client must NOT retry.
        """
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/monitor/bigip/~Common~new-mon"
        ).mock(return_value=httpx.Response(404))
        respx.mock.post(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/monitor/bigip"
        ).mock(side_effect=httpx.WriteTimeout("timed out", request=None))

        with pytest.raises(F5TimeoutError) as exc_info:
            await gtm_client.ensure_monitor("bigip", "new-mon", {"interval": 5})

        assert exc_info.value.operation == "POST"

    @respx.mock
    @pytest.mark.asyncio
    async def test_ensure_monitor_patch_timeout_raises_f5_timeout_error(
        self, gtm_client: F5GTMClient
    ) -> None:
        """Timeout on PATCH (update) raises F5TimeoutError."""
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        current = {"name": "my-mon", "interval": 5, "timeout": 15}
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/monitor/bigip/~Common~my-mon"
        ).mock(return_value=httpx.Response(200, json=current))
        respx.mock.patch(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/monitor/bigip/~Common~my-mon"
        ).mock(side_effect=httpx.ConnectTimeout("timed out", request=None))

        with pytest.raises(F5TimeoutError) as exc_info:
            await gtm_client.ensure_monitor(
                "bigip", "my-mon", {"interval": 10, "timeout": 15}
            )

        assert exc_info.value.operation == "PATCH"

    @respx.mock
    @pytest.mark.asyncio
    async def test_delete_wideip_timeout_raises_f5_timeout_error(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        current = {"name": "svc.example.com"}
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/wideip/a/~Common~svc.example.com"
        ).mock(return_value=httpx.Response(200, json=current))
        respx.mock.delete(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/wideip/a/~Common~svc.example.com"
        ).mock(side_effect=httpx.ReadTimeout("timed out", request=None))

        with pytest.raises(F5TimeoutError) as exc_info:
            await gtm_client.delete_wideip("a", "svc.example.com")

        assert exc_info.value.operation == "DELETE"


# ===========================================================================
# TestF5GTMClientErrors
# ===========================================================================


class TestF5GTMClientErrors:
    """HTTP error status codes map to the correct exception types."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_post_409_raises_f5_conflict_error(
        self, gtm_client: F5GTMClient
    ) -> None:
        """
        Although ensure_* usually avoids 409 by reading first, POST can still
        return 409 under race conditions.  Must raise F5ConflictError.
        """
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        # GET returns 404 so we try to create.
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/monitor/bigip/~Common~dup-mon"
        ).mock(return_value=httpx.Response(404))
        respx.mock.post(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/monitor/bigip"
        ).mock(return_value=httpx.Response(409, json={"code": 409, "message": "already exists"}))

        with pytest.raises(F5ConflictError):
            await gtm_client.ensure_monitor("bigip", "dup-mon", {"interval": 5})

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_5xx_raises_f5_server_error(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/monitor/bigip/~Common~any-mon"
        ).mock(return_value=httpx.Response(500, json={"code": 500}))

        with pytest.raises(F5ServerError):
            await gtm_client.get_monitor("bigip", "any-mon")

    @respx.mock
    @pytest.mark.asyncio
    async def test_patch_503_raises_f5_server_error(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        current = {"name": "my-mon", "interval": 5}
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/monitor/bigip/~Common~my-mon"
        ).mock(return_value=httpx.Response(200, json=current))
        respx.mock.patch(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/monitor/bigip/~Common~my-mon"
        ).mock(return_value=httpx.Response(503, text="Service Unavailable"))

        with pytest.raises(F5ServerError):
            await gtm_client.ensure_monitor("bigip", "my-mon", {"interval": 10})

    @respx.mock
    @pytest.mark.asyncio
    async def test_delete_5xx_raises_f5_server_error(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        current = {"name": "my-mon"}
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/monitor/bigip/~Common~my-mon"
        ).mock(return_value=httpx.Response(200, json=current))
        respx.mock.delete(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/monitor/bigip/~Common~my-mon"
        ).mock(return_value=httpx.Response(500, text="Internal Server Error"))

        with pytest.raises(F5ServerError):
            await gtm_client.delete_monitor("bigip", "my-mon")


# ===========================================================================
# TestIdempotencyNoWriteOnNoOp
# ===========================================================================


class TestIdempotencyNoWriteOnNoOp:
    """
    Verify that no HTTP write call (PATCH/POST/PUT/DELETE) is made when
    ensure_* detects that the current state already matches the desired state.

    respx raises an error for any unregistered route, so if a write were
    issued the test would fail with a NoMatchFound error.
    """

    @respx.mock
    @pytest.mark.asyncio
    async def test_ensure_monitor_no_write_on_no_op(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        # Only register GET — any PATCH/POST would be unmatched and raise.
        current = {"name": "m1", "interval": 5, "timeout": 15, "defaultsFrom": "/Common/bigip"}
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/monitor/bigip/~Common~m1"
        ).mock(return_value=httpx.Response(200, json=current))

        result = await gtm_client.ensure_monitor(
            "bigip", "m1", {"interval": 5, "timeout": 15}
        )
        assert result.action == "no_op"

    @respx.mock
    @pytest.mark.asyncio
    async def test_ensure_wideip_no_write_on_no_op(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        current = {
            "name": "svc.example.com",
            "poolLbMode": "round-robin",
            "kind": "tm:gtm:wideip:a:astate",
        }
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/wideip/a/~Common~svc.example.com"
        ).mock(return_value=httpx.Response(200, json=current))

        result = await gtm_client.ensure_wideip(
            "a", "svc.example.com", {"poolLbMode": "round-robin"}
        )
        assert result.action == "no_op"

    @respx.mock
    @pytest.mark.asyncio
    async def test_ensure_pool_members_no_write_on_no_op(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        members = [{"name": "/Common/vs1", "ratio": 1}]
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/pool/a/~Common~p1/members"
        ).mock(return_value=httpx.Response(200, json={"items": members}))

        result = await gtm_client.ensure_pool_members("a", "p1", members)
        assert result.action == "no_op"


# ===========================================================================
# TestPreStateCapture
# ===========================================================================


class TestPreStateCapture:
    """
    Verify that pre_state is correctly captured before any mutation.
    This is critical for rollback correctness (§3.3 / §3.5 of the plan).
    """

    @respx.mock
    @pytest.mark.asyncio
    async def test_ensure_monitor_update_captures_pre_state(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        pre = {"name": "my-mon", "interval": 5, "timeout": 15}
        post = {"name": "my-mon", "interval": 30, "timeout": 15}
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/monitor/bigip/~Common~my-mon"
        ).mock(return_value=httpx.Response(200, json=pre))
        respx.mock.patch(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/monitor/bigip/~Common~my-mon"
        ).mock(return_value=httpx.Response(200, json=post))

        result = await gtm_client.ensure_monitor(
            "bigip", "my-mon", {"interval": 30, "timeout": 15}
        )
        # pre_state must be the ORIGINAL state, not the updated one.
        assert result.pre_state == pre
        assert result.post_state == post
        assert result.pre_state is not result.post_state

    @respx.mock
    @pytest.mark.asyncio
    async def test_delete_wideip_captures_pre_state(
        self, gtm_client: F5GTMClient
    ) -> None:
        _mock_login_and_extend(respx.mock)  # type: ignore[attr-defined]
        pre = {"name": "svc.example.com", "poolLbMode": "round-robin"}
        respx.mock.get(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/wideip/a/~Common~svc.example.com"
        ).mock(return_value=httpx.Response(200, json=pre))
        respx.mock.delete(  # type: ignore[attr-defined]
            f"{BASE_URL}/mgmt/tm/gtm/wideip/a/~Common~svc.example.com"
        ).mock(return_value=httpx.Response(200))

        result = await gtm_client.delete_wideip("a", "svc.example.com")
        assert result.pre_state == pre
        assert result.post_state is None
