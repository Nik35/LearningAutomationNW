"""
app/clients/f5/auth.py
======================
Token-based authentication for the F5 iControl REST API.

Protocol (from api-research-findings.md):
  1. POST /mgmt/shared/authn/login  →  response["token"]["token"]
  2. PATCH /mgmt/shared/authz/tokens/{token_value}  →  extend to 36 000 s
  3. Subsequent calls carry  X-F5-Auth-Token: {token_value}

Caching / stampede prevention:
  - Token is stored in Redis as a JSON string keyed by device_id.
  - Redis TTL = TOKEN_EXTEND_SECONDS - TOKEN_REFRESH_BEFORE_EXPIRY_SECONDS
    so the key disappears just before we'd need to refresh.
  - A Redis SET NX lock prevents concurrent workers from obtaining multiple
    tokens simultaneously (stampede guard).
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import httpx

from app.clients.f5.session import F5Session


class F5TokenManager:
    """
    Manages a cached, long-lived F5 auth token per device.

    Parameters
    ----------
    redis_client:
        Async Redis client (``redis.asyncio.Redis``).
    device_id:
        Logical identifier for the device.  Used as part of Redis keys.
    username:
        F5 admin username.
    password:
        F5 admin password.
    session:
        The ``F5Session`` instance for this device.  The token manager
        calls ``/mgmt/shared/authn/login`` and
        ``/mgmt/shared/authz/tokens/{token}`` through it.
    """

    TOKEN_REDIS_KEY = "f5:token:{device_id}"
    LOCK_REDIS_KEY = "f5:token:lock:{device_id}"

    # From api-research-findings.md: max lifetime the F5 allows.
    TOKEN_EXTEND_SECONDS: int = 36_000

    # Refresh this many seconds before the token would expire.
    TOKEN_REFRESH_BEFORE_EXPIRY_SECONDS: int = 120

    # Redis TTL for the cached token entry.  The key disappears just before
    # we'd want to refresh, preventing stale-token usage.
    _CACHE_TTL: int = TOKEN_EXTEND_SECONDS - TOKEN_REFRESH_BEFORE_EXPIRY_SECONDS  # = 35 880

    # Short TTL for the distributed lock — just long enough to cover one
    # login + extend round-trip.
    _LOCK_TTL_SECONDS: int = 5

    def __init__(
        self,
        redis_client: Any,
        device_id: str,
        username: str,
        password: str,
        session: F5Session,
        login_provider_name: str = "tmos",
    ) -> None:
        self._redis = redis_client
        self._device_id = device_id
        self._username = username
        self._password = password
        self._session = session
        # For TACACS+ auth, set this to the name of the TACACS+ auth source
        # configured on the BIG-IP (check: tmsh list auth tacacs).
        # For local auth use "tmos". Injected from settings.F5_LOGIN_PROVIDER_NAME.
        self._login_provider_name = login_provider_name

        self._token_key = self.TOKEN_REDIS_KEY.format(device_id=device_id)
        self._lock_key = self.LOCK_REDIS_KEY.format(device_id=device_id)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def get_token(self) -> str:
        """
        Return a valid auth token, obtaining or refreshing one if necessary.

        Algorithm:
          1. Read cached token from Redis.  If it exists and has
             > TOKEN_REFRESH_BEFORE_EXPIRY_SECONDS remaining, return it.
          2. Acquire a Redis NX lock to prevent concurrent refresh.
          3. Re-check the cache (another worker may have refreshed while
             we waited for the lock).
          4. Login to F5, extend the token lifetime to TOKEN_EXTEND_SECONDS,
             cache the result in Redis with TTL = _CACHE_TTL.
          5. Release the lock and return the token.

        Raises
        ------
        httpx.HTTPStatusError
            If the login or token-extension call returns a non-2xx status.
        httpx.TimeoutException
            If the login call times out.  Callers should treat this as an
            unknown outcome (per the plan's timeout rule) rather than
            retrying blindly.
        """
        cached = await self._read_cached_token()
        if cached is not None:
            return cached

        # Acquire the distributed lock before issuing a login call.
        acquired = await self._acquire_lock()
        if not acquired:
            # Another worker holds the lock.  Wait briefly, then re-read
            # the cache — the other worker will have populated it.
            await asyncio.sleep(self._LOCK_TTL_SECONDS)
            cached = await self._read_cached_token()
            if cached is not None:
                return cached
            # If still not cached after the wait, acquire the lock ourselves
            # (the other worker may have crashed).
            acquired = await self._acquire_lock()
            if not acquired:
                # Give up waiting; let the caller surface an error.
                raise RuntimeError(
                    f"Could not acquire F5 token lock for device {self._device_id}"
                )

        try:
            # Double-check after acquiring the lock.
            cached = await self._read_cached_token()
            if cached is not None:
                return cached

            token = await self._login_and_extend()
            await self._cache_token(token)
            return token
        finally:
            await self._release_lock()

    async def inject_auth(self, headers: dict[str, str]) -> dict[str, str]:
        """
        Return a copy of ``headers`` with the ``X-F5-Auth-Token`` header
        added (or replaced).
        """
        token = await self.get_token()
        return {**headers, "X-F5-Auth-Token": token}

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _read_cached_token(self) -> str | None:
        """
        Return the cached token if it still has enough lifetime remaining.
        Returns ``None`` if the entry is absent, malformed, or too close to
        expiry.
        """
        raw: bytes | None = await self._redis.get(self._token_key)
        if raw is None:
            return None
        try:
            data: dict[str, Any] = json.loads(raw)
            token: str = data["token"]
            expires_at: float = float(data["expires_at_unix"])
        except (KeyError, ValueError, TypeError):
            # Corrupt cache entry — treat as a miss.
            return None

        remaining = expires_at - time.time()
        if remaining > self.TOKEN_REFRESH_BEFORE_EXPIRY_SECONDS:
            return token
        return None

    async def _cache_token(self, token: str) -> None:
        """Write the token to Redis with the appropriate TTL."""
        expires_at = time.time() + self.TOKEN_EXTEND_SECONDS
        payload = json.dumps({"token": token, "expires_at_unix": expires_at})
        await self._redis.set(self._token_key, payload, ex=self._CACHE_TTL)

    async def _acquire_lock(self) -> bool:
        """
        Attempt to acquire the Redis NX lock.

        Returns ``True`` if acquired, ``False`` if another holder owns it.
        """
        result = await self._redis.set(
            self._lock_key,
            "1",
            nx=True,
            ex=self._LOCK_TTL_SECONDS,
        )
        return result is not None

    async def _release_lock(self) -> None:
        """Release the Redis NX lock unconditionally."""
        await self._redis.delete(self._lock_key)

    async def _login_and_extend(self) -> str:
        """
        Perform the two-step F5 token acquisition:
          1. POST /mgmt/shared/authn/login  →  extract token string
          2. PATCH /mgmt/shared/authz/tokens/{token}  →  extend lifetime

        Returns the token string.

        Field names are taken verbatim from api-research-findings.md.
        """
        # Step 1: Login.
        login_response = await self._session.request(
            "POST",
            "/mgmt/shared/authn/login",
            json={
                "username": self._username,
                "password": self._password,
                "loginProviderName": self._login_provider_name,
            },
        )
        login_response.raise_for_status()
        body = login_response.json()
        token: str = body["token"]["token"]

        # Step 2: Extend the token lifetime to the maximum allowed.
        extend_response = await self._session.request(
            "PATCH",
            f"/mgmt/shared/authz/tokens/{token}",
            json={"timeout": self.TOKEN_EXTEND_SECONDS},
            headers={"X-F5-Auth-Token": token},
        )
        extend_response.raise_for_status()

        return token
