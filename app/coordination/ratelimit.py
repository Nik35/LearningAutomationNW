"""
ratelimit.py — per-device token-bucket rate limiter.

All Redis state is managed atomically via token_bucket.lua.
No Python-level read-modify-write is performed.

Architecture invariants (CLAUDE.md):
- Rate limiting is scoped per target device.
- Redis unavailable → RedisUnavailableError (fail closed).
- OOM on write → RedisOOMError (503 + Retry-After).

Token bucket key layout:
    key    = ``bucket:{device_id}``
    fields = tokens (current count), last_refill (Unix timestamp float)
"""

from __future__ import annotations

import asyncio
import os
import time

import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from app.coordination.exceptions import RedisOOMError, RedisUnavailableError

# ---------------------------------------------------------------------------
# Lua script loader
# ---------------------------------------------------------------------------

_SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), "scripts")


def _load_script(name: str) -> str:
    path = os.path.join(_SCRIPTS_DIR, name)
    with open(path, encoding="utf-8") as fh:
        return fh.read()


_TOKEN_BUCKET_SCRIPT = _load_script("token_bucket.lua")


# ---------------------------------------------------------------------------
# Redis error wrapper
# ---------------------------------------------------------------------------

def _wrap_redis_error(exc: Exception) -> None:
    if isinstance(exc, (RedisConnectionError, RedisTimeoutError)):
        raise RedisUnavailableError(f"Redis unavailable: {exc}") from exc
    if isinstance(exc, ResponseError) and "OOM" in str(exc):
        raise RedisOOMError(f"Redis out of memory: {exc}") from exc
    raise exc


# ---------------------------------------------------------------------------
# DeviceTokenBucket
# ---------------------------------------------------------------------------

class DeviceTokenBucket:
    """
    Per-device token-bucket rate limiter.

    Implements D-6 (token bucket per device) via atomic Lua script.
    Permits controlled burst then settles to a sustained refill rate.

    Redis key: ``bucket:{device_id}``

    Parameters
    ----------
    redis_client:
        An ``redis.asyncio.Redis`` instance.
    device_id:
        Identifies the target F5 device.  Always part of the Redis key.
    capacity:
        P-2 — maximum token count (burst ceiling).
        # TODO: awaiting T-0.7
    refill_rate:
        P-3 — tokens added per second during steady state.
        # TODO: awaiting T-0.7
    """

    def __init__(
        self,
        redis_client: aioredis.Redis,
        device_id: str,
        capacity: int,        # P-2: token bucket size — awaiting T-0.7
        refill_rate: float,   # P-3: refill rate per second — awaiting T-0.7
    ) -> None:
        self._redis = redis_client
        self._device_id = device_id
        self._capacity = capacity
        self._refill_rate = refill_rate
        self._key = f"bucket:{device_id}"

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    async def consume(self, tokens: int = 1) -> bool:
        """
        Atomically attempt to consume ``tokens`` from the bucket.

        Returns
        -------
        True   — request allowed; tokens deducted.
        False  — bucket empty; request rejected.

        Raises
        ------
        RedisUnavailableError
            If Redis cannot be reached (fail closed per D-4).
        RedisOOMError
            If Redis returns an OOM error.
        """
        now = time.time()
        try:
            result = await self._redis.eval(  # type: ignore[attr-defined]
                _TOKEN_BUCKET_SCRIPT,
                1,
                self._key,
                self._capacity,
                self._refill_rate,
                tokens,
                now,
            )
            return bool(result)
        except (RedisConnectionError, RedisTimeoutError) as exc:
            raise RedisUnavailableError(f"Redis unavailable: {exc}") from exc
        except ResponseError as exc:
            if "OOM" in str(exc):
                raise RedisOOMError(f"Redis out of memory: {exc}") from exc
            raise

    async def wait_and_consume(
        self,
        tokens: int = 1,
        timeout: float = 30.0,
    ) -> bool:
        """
        Poll until a token is available or the timeout elapses.

        Parameters
        ----------
        tokens:
            How many tokens to consume (default 1).
        timeout:
            Maximum wait time in seconds.

        Returns
        -------
        True  — tokens consumed within the timeout.
        False — timeout elapsed without tokens becoming available.

        Raises
        ------
        RedisUnavailableError
            If Redis cannot be reached.
        RedisOOMError
            If Redis returns an OOM error.
        """
        deadline = time.monotonic() + timeout
        # Derive a reasonable poll interval from the refill rate:
        # wait roughly the time it takes to refill one token, but cap at 2s.
        if self._refill_rate > 0:
            base_interval = min(1.0 / self._refill_rate, 2.0)
        else:
            base_interval = 1.0

        while True:
            if await self.consume(tokens):
                return True

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False

            await asyncio.sleep(min(base_interval, remaining))
