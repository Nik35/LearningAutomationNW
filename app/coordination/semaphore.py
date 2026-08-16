"""
semaphore.py — per-device concurrency slot management.

Every Redis operation is atomic via Lua scripts loaded from the scripts/
directory.  No Python-level read-modify-write is performed.

Architecture invariants (CLAUDE.md):
- Concurrency is scoped per target device; device_id is always part of the key.
- Redis unavailable → RedisUnavailableError (fail closed).
- OOM on write → RedisOOMError (503 + Retry-After).
- The slot's TTL is the safety net: a dead worker's slot expires automatically.
  A live worker renews the TTL via heartbeat (calls renew() on each heartbeat).
"""

from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from typing import AsyncIterator

import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from app.coordination.exceptions import RedisOOMError, RedisUnavailableError

# ---------------------------------------------------------------------------
# Path helpers — load Lua scripts relative to this file.
# ---------------------------------------------------------------------------

_SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), "scripts")


def _load_script(name: str) -> str:
    path = os.path.join(_SCRIPTS_DIR, name)
    with open(path, encoding="utf-8") as fh:
        return fh.read()


_ACQUIRE_SCRIPT = _load_script("semaphore_acquire.lua")
_RELEASE_SCRIPT = _load_script("semaphore_release.lua")
_RENEW_SCRIPT = _load_script("semaphore_renew.lua")


# ---------------------------------------------------------------------------
# Redis error wrapper
# ---------------------------------------------------------------------------

def _wrap_redis_error(exc: Exception) -> None:
    """Re-raise a Redis exception as a coordination-layer exception."""
    if isinstance(exc, (RedisConnectionError, RedisTimeoutError)):
        raise RedisUnavailableError(
            f"Redis unavailable: {exc}"
        ) from exc
    if isinstance(exc, ResponseError) and "OOM" in str(exc):
        raise RedisOOMError(
            f"Redis out of memory: {exc}"
        ) from exc
    raise exc


# ---------------------------------------------------------------------------
# DeviceSemaphore
# ---------------------------------------------------------------------------

class DeviceSemaphore:
    """
    Per-device concurrency semaphore backed by a Redis Hash.

    Each slot is a field in a Redis Hash:
        key   = ``sem:{device_id}``
        field = worker_id
        value = acquired_at (Unix timestamp)

    The entire hash has a TTL (``slot_ttl``).  A live worker renews this TTL
    periodically via :meth:`renew`.  If a worker dies, the key expires and
    all its slots are reclaimed automatically — no explicit cleanup needed.

    Parameters
    ----------
    redis_client:
        An ``redis.asyncio.Redis`` instance (or compatible async client).
    device_id:
        Identifies the target F5 device.  Always part of the Redis key.
    max_slots:
        P-1 — per-device concurrency limit.  Value must come from WP-0
        measurements (T-0.6, T-0.7).  # TODO: awaiting T-0.6/T-0.7
    slot_ttl:
        TTL in seconds applied to the semaphore hash.  Must cover the
        maximum expected step duration plus heartbeat jitter.
        # TODO: awaiting WP-0 to derive from P-4 / P-5
    """

    def __init__(
        self,
        redis_client: aioredis.Redis,
        device_id: str,
        max_slots: int,     # P-1: per-device concurrency — awaiting T-0.6/T-0.7
        slot_ttl: int,      # derived from P-4 / P-5 — awaiting T-0.6/T-0.7
    ) -> None:
        self._redis = redis_client
        self._device_id = device_id
        self._max_slots = max_slots
        self._slot_ttl = slot_ttl
        self._key = f"sem:{device_id}"

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    async def acquire(self, worker_id: str, timeout_seconds: float) -> bool:
        """
        Try to acquire a slot, polling until one is free or timeout expires.

        Parameters
        ----------
        worker_id:
            Unique identifier for this worker (e.g. Celery task ID + pod ID).
        timeout_seconds:
            P-4 — maximum time to wait for a slot.  Value supplied by caller
            from config; never hardcoded here.  # TODO: awaiting T-0.6/T-0.7

        Returns
        -------
        True if the slot was acquired, False if the timeout elapsed.

        Raises
        ------
        RedisUnavailableError
            If Redis cannot be reached.
        RedisOOMError
            If Redis returns an OOM error.
        """
        deadline = time.monotonic() + timeout_seconds
        # Poll interval: start at 100 ms, cap at 2 s to avoid hammering Redis.
        poll_interval = 0.1
        max_poll_interval = 2.0

        while True:
            acquired = await self._run_acquire(worker_id)
            if acquired:
                return True

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False

            await asyncio.sleep(min(poll_interval, remaining))
            poll_interval = min(poll_interval * 1.5, max_poll_interval)

    async def release(self, worker_id: str) -> None:
        """
        Release a slot.  Idempotent: safe to call even if the slot already
        expired or was released.

        Raises
        ------
        RedisUnavailableError
            If Redis cannot be reached.
        """
        try:
            await self._redis.eval(  # type: ignore[attr-defined]
                _RELEASE_SCRIPT,
                1,
                self._key,
                worker_id,
            )
        except (RedisConnectionError, RedisTimeoutError, ResponseError) as exc:
            _wrap_redis_error(exc)

    async def renew(self, worker_id: str, new_ttl: int | None = None) -> bool:
        """
        Heartbeat renewal: refresh the TTL on the semaphore hash if the slot
        still exists.

        Parameters
        ----------
        worker_id:
            The worker whose slot is being renewed.
        new_ttl:
            Override the TTL (seconds).  Defaults to the original ``slot_ttl``.

        Returns
        -------
        True if the field was found and TTL refreshed.
        False if the slot was already reclaimed (TTL expired).

        Raises
        ------
        RedisUnavailableError
            If Redis cannot be reached.
        """
        ttl = new_ttl if new_ttl is not None else self._slot_ttl
        try:
            result = await self._redis.eval(  # type: ignore[attr-defined]
                _RENEW_SCRIPT,
                1,
                self._key,
                worker_id,
                ttl,
            )
            return bool(result)
        except (RedisConnectionError, RedisTimeoutError, ResponseError) as exc:
            _wrap_redis_error(exc)
            return False  # unreachable; _wrap_redis_error always raises

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def slot(
        self,
        worker_id: str,
        timeout_seconds: float,
    ) -> AsyncIterator[bool]:
        """
        Async context manager that acquires on enter and releases in finally.

        Usage::

            async with semaphore.slot(worker_id, timeout_seconds=P4) as acquired:
                if not acquired:
                    # re-enqueue with backoff
                    return
                # do work

        The release in ``finally`` is unconditional.  Even if the workflow
        raises an unhandled exception, the slot is freed.

        Yields
        ------
        bool
            True if the slot was acquired, False if the timeout elapsed.
        """
        acquired = await self.acquire(worker_id, timeout_seconds)
        try:
            yield acquired
        finally:
            if acquired:
                await self.release(worker_id)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _run_acquire(self, worker_id: str) -> bool:
        """Execute the acquire Lua script once.  Returns True if acquired."""
        try:
            result = await self._redis.eval(  # type: ignore[attr-defined]
                _ACQUIRE_SCRIPT,
                1,
                self._key,
                self._max_slots,
                self._slot_ttl,
                worker_id,
            )
            return bool(result)
        except (RedisConnectionError, RedisTimeoutError) as exc:
            raise RedisUnavailableError(f"Redis unavailable: {exc}") from exc
        except ResponseError as exc:
            if "OOM" in str(exc):
                raise RedisOOMError(f"Redis out of memory: {exc}") from exc
            raise
