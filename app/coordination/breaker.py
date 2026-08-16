"""
breaker.py — per-device circuit breaker.

All Redis state is managed atomically via Lua scripts.
No Python-level read-modify-write is performed.

Architecture invariants (CLAUDE.md / WP-3, T-3.4):
- Breaker is per device; a failing device does not affect others (D-5).
- When open, requests stay QUEUED — they do not fail.
- State is in Redis, visible to all pods (cross-pod consistency).
- Half-open probes are serialised: exactly one probe fires at a time.

Redis key layout for device ``D``:
    breaker:D:stats   — Hash; aggregate stats bucket (currently unused but
                        reserved for future metrics export)
    breaker:D:events  — List; sliding window of event entries
    breaker:D:state   — String ("open"); exists only when breaker is open
    breaker:D:probe   — String ("1"); exists only when a probe is in flight

Thresholds (all P-10, from T-0.6 / T-0.7):
    error_rate_threshold         fraction of calls that may fail before tripping
    p95_latency_threshold_ms     approx p95 latency ceiling
    consecutive_timeout_threshold  (enforced via timeout_rate in sliding window)
"""

from __future__ import annotations

import os
import time
from enum import Enum

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


_RECORD_SCRIPT = _load_script("breaker_record.lua")
_PROBE_SCRIPT = _load_script("breaker_probe.lua")


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
# BreakerState
# ---------------------------------------------------------------------------

class BreakerState(str, Enum):
    """
    Circuit breaker states.

    CLOSED    — normal operation; all requests proceed.
    OPEN      — device is considered unhealthy; requests stay QUEUED.
    HALF_OPEN — one probe request may proceed to test device health.
    """

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


# ---------------------------------------------------------------------------
# DeviceCircuitBreaker
# ---------------------------------------------------------------------------

class DeviceCircuitBreaker:
    """
    Per-device circuit breaker with sliding-window statistics.

    Transitions: CLOSED → OPEN on threshold breach.
                 OPEN → HALF_OPEN when the self-healing TTL expires and a probe
                        is attempted.
                 HALF_OPEN → CLOSED on successful probe (via reset()).
                 HALF_OPEN → OPEN if the probe fails (state key persists).

    Parameters
    ----------
    redis_client:
        An ``redis.asyncio.Redis`` instance.
    device_id:
        Identifies the target F5 device.  Always part of Redis keys.
    error_rate_threshold:
        P-10 — fraction [0.0, 1.0] of calls that may be errors before tripping.
        # TODO: awaiting T-0.6/T-0.7
    p95_latency_threshold_ms:
        P-10 — approximate p95 latency in ms above which the breaker trips.
        # TODO: awaiting T-0.6/T-0.7
    consecutive_timeout_threshold:
        P-10 — consecutive timeouts as a fraction of total calls in the window.
        Used as a timeout rate threshold, not a raw count, to remain meaningful
        across different window sizes.
        # TODO: awaiting T-0.6/T-0.7
    window_seconds:
        Sliding window duration for statistics.  Older events are discarded.
        # TODO: awaiting T-0.6/T-0.7
    half_open_probe_ttl:
        Seconds the probe key lives before another probe is allowed.
        # TODO: awaiting T-0.6/T-0.7
    open_state_ttl:
        TTL for the "open" state key.  When it expires, the breaker
        auto-transitions to allowing probes (HALF_OPEN logic).
        # TODO: awaiting T-0.6/T-0.7
    """

    def __init__(
        self,
        redis_client: aioredis.Redis,
        device_id: str,
        error_rate_threshold: float,         # P-10 — awaiting T-0.6/T-0.7
        p95_latency_threshold_ms: float,     # P-10 — awaiting T-0.6/T-0.7
        consecutive_timeout_threshold: int,  # P-10 — awaiting T-0.6/T-0.7
        window_seconds: int,                 # P-10 — awaiting T-0.6/T-0.7
        half_open_probe_ttl: int,            # P-10 — awaiting T-0.6/T-0.7
        open_state_ttl: int,                 # P-10 — awaiting T-0.6/T-0.7
    ) -> None:
        self._redis = redis_client
        self._device_id = device_id
        self._error_rate_threshold = error_rate_threshold
        self._p95_latency_threshold_ms = p95_latency_threshold_ms
        # Stored as a fraction for the Lua script (consistent with error_rate).
        # We derive a timeout_rate_threshold from the consecutive count:
        # if N consecutive timeouts should trip the breaker, then within the
        # sliding window a timeout_rate > N/window_total is our proxy.
        # The Lua script accepts a rate, so we pass consecutive_timeout_threshold
        # directly as a raw count turned into a fraction at threshold=1/window.
        # In practice callers supply a fractional threshold derived from WP-0.
        self._consecutive_timeout_threshold = consecutive_timeout_threshold
        self._window_seconds = window_seconds
        self._half_open_probe_ttl = half_open_probe_ttl
        self._open_state_ttl = open_state_ttl

        # Redis key prefixes — device_id scoped.
        self._stats_key = f"breaker:{device_id}:stats"
        self._events_key = f"breaker:{device_id}:events"
        self._state_key = f"breaker:{device_id}:state"
        self._probe_key = f"breaker:{device_id}:probe"

    # ------------------------------------------------------------------
    # Recording outcomes
    # ------------------------------------------------------------------

    async def record_success(self, latency_ms: float) -> None:
        """
        Record a successful call.

        Raises
        ------
        RedisUnavailableError / RedisOOMError on Redis errors.
        """
        await self._record("success", latency_ms)

    async def record_failure(self, latency_ms: float) -> None:
        """
        Record a failed call (non-timeout error, e.g. HTTP 5xx).

        Raises
        ------
        RedisUnavailableError / RedisOOMError on Redis errors.
        """
        await self._record("failure", latency_ms)

    async def record_timeout(self) -> None:
        """
        Record a timeout.

        Per §7 failure-matrix row 7: a timeout means the outcome is *unknown*.
        The caller must read back to determine actual state before acting.
        This method only records the timeout event for breaker statistics.

        Latency is set to 0 for timeouts (actual duration is unknown).

        Raises
        ------
        RedisUnavailableError / RedisOOMError on Redis errors.
        """
        await self._record("timeout", 0.0)

    # ------------------------------------------------------------------
    # State queries
    # ------------------------------------------------------------------

    async def get_state(self) -> BreakerState:
        """
        Query the current breaker state for this device.

        This method runs the probe script, which means it may transition the
        breaker from OPEN to HALF_OPEN atomically if the state key exists but
        no probe is in flight.

        To query state without side effects, use :meth:`peek_state`.

        Returns
        -------
        BreakerState

        Raises
        ------
        RedisUnavailableError / RedisOOMError on Redis errors.
        """
        try:
            result = await self._redis.eval(  # type: ignore[attr-defined]
                _PROBE_SCRIPT,
                2,
                self._state_key,
                self._probe_key,
                self._half_open_probe_ttl,
            )
            raw = result.decode() if isinstance(result, bytes) else str(result)
            return BreakerState(raw)
        except (RedisConnectionError, RedisTimeoutError) as exc:
            raise RedisUnavailableError(f"Redis unavailable: {exc}") from exc
        except ResponseError as exc:
            if "OOM" in str(exc):
                raise RedisOOMError(f"Redis out of memory: {exc}") from exc
            raise

    async def peek_state(self) -> BreakerState:
        """
        Non-destructive state check: returns the current state without
        attempting to claim a probe slot.

        Useful for admission control (§3.1 step 5d) where we only want to
        know whether the breaker is open, not to initiate a probe.

        Returns
        -------
        BreakerState

        Raises
        ------
        RedisUnavailableError / RedisOOMError on Redis errors.
        """
        try:
            state_exists = await self._redis.exists(self._state_key)
        except (RedisConnectionError, RedisTimeoutError) as exc:
            raise RedisUnavailableError(f"Redis unavailable: {exc}") from exc
        except ResponseError as exc:
            if "OOM" in str(exc):
                raise RedisOOMError(f"Redis out of memory: {exc}") from exc
            raise

        if not state_exists:
            return BreakerState.CLOSED

        # Check if a probe is in flight to determine open vs half_open.
        try:
            probe_exists = await self._redis.exists(self._probe_key)
        except (RedisConnectionError, RedisTimeoutError) as exc:
            raise RedisUnavailableError(f"Redis unavailable: {exc}") from exc

        if probe_exists:
            return BreakerState.HALF_OPEN
        return BreakerState.OPEN

    # ------------------------------------------------------------------
    # Manual reset (ops)
    # ------------------------------------------------------------------

    async def reset(self) -> None:
        """
        Manually reset the breaker to CLOSED.

        Deletes both the state key and the probe key.  This is the correct
        action after a successful probe confirms the device is healthy.

        Also used by operators to force-close a breaker (e.g. after manual
        device maintenance).

        Raises
        ------
        RedisUnavailableError / RedisOOMError on Redis errors.
        """
        try:
            await self._redis.delete(self._state_key, self._probe_key)
        except (RedisConnectionError, RedisTimeoutError) as exc:
            raise RedisUnavailableError(f"Redis unavailable: {exc}") from exc
        except ResponseError as exc:
            if "OOM" in str(exc):
                raise RedisOOMError(f"Redis out of memory: {exc}") from exc
            raise

    async def release_probe(self) -> None:
        """
        Release the probe slot without resetting the breaker.

        Called when a probe fails: the state key stays (breaker remains open),
        but the probe key is removed so another probe can fire after
        ``half_open_probe_ttl`` seconds.

        Raises
        ------
        RedisUnavailableError / RedisOOMError on Redis errors.
        """
        try:
            await self._redis.delete(self._probe_key)
        except (RedisConnectionError, RedisTimeoutError) as exc:
            raise RedisUnavailableError(f"Redis unavailable: {exc}") from exc
        except ResponseError as exc:
            if "OOM" in str(exc):
                raise RedisOOMError(f"Redis out of memory: {exc}") from exc
            raise

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _record(self, outcome: str, latency_ms: float) -> str:
        """
        Run breaker_record.lua and return the resulting state string.

        The timeout_rate_threshold passed to Lua is derived from
        ``consecutive_timeout_threshold`` as a fraction:
            timeout_rate_threshold = consecutive_timeout_threshold / window_seconds
        This is an approximation; for exact consecutive counting a separate
        counter key would be needed, but this is adequate for P-10 calibration.
        """
        now = int(time.time())
        # Derive a timeout_rate threshold from the consecutive count and window.
        # Guard against division by zero.
        if self._window_seconds > 0:
            timeout_rate_threshold = (
                self._consecutive_timeout_threshold / self._window_seconds
            )
        else:
            timeout_rate_threshold = 1.0  # never trip; degenerate case

        try:
            result = await self._redis.eval(  # type: ignore[attr-defined]
                _RECORD_SCRIPT,
                3,
                self._stats_key,
                self._events_key,
                self._state_key,
                outcome,
                latency_ms,
                now,
                self._window_seconds,
                self._error_rate_threshold,
                timeout_rate_threshold,
                self._p95_latency_threshold_ms,
                self._open_state_ttl,
            )
            raw = result.decode() if isinstance(result, bytes) else str(result)
            return raw
        except (RedisConnectionError, RedisTimeoutError) as exc:
            raise RedisUnavailableError(f"Redis unavailable: {exc}") from exc
        except ResponseError as exc:
            if "OOM" in str(exc):
                raise RedisOOMError(f"Redis out of memory: {exc}") from exc
            raise
