"""
tests/unit/test_coordination.py
================================
Unit tests for the coordination layer (semaphore, rate limiter, circuit breaker).

Uses ``fakeredis[lua]`` for full Lua script evaluation without a real Redis
instance.  All P-n parameters are supplied explicitly; none are hardcoded.

Test organisation:
    TestDeviceSemaphore         — slot acquisition, limits, release, expiry
    TestDeviceTokenBucket       — burst, sustained rate, rejection
    TestDeviceCircuitBreaker    — closed→open, half-open probe, reset on success
    TestRedisUnavailable        — all primitives raise RedisUnavailableError
"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis
import fakeredis.aioredis
import pytest
import pytest_asyncio

from app.coordination.breaker import BreakerState, DeviceCircuitBreaker
from app.coordination.exceptions import RedisOOMError, RedisUnavailableError
from app.coordination.ratelimit import DeviceTokenBucket
from app.coordination.semaphore import DeviceSemaphore

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def fake_redis() -> fakeredis.aioredis.FakeRedis:
    """A fresh in-memory Redis instance with Lua support for each test."""
    return fakeredis.aioredis.FakeRedis(decode_responses=False)


@pytest.fixture()
def device_id() -> str:
    return "dev-gtm-01"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_semaphore(
    redis_client: fakeredis.aioredis.FakeRedis,
    device_id: str,
    max_slots: int = 2,
    slot_ttl: int = 30,
) -> DeviceSemaphore:
    return DeviceSemaphore(
        redis_client=redis_client,
        device_id=device_id,
        max_slots=max_slots,
        slot_ttl=slot_ttl,
    )


def _make_bucket(
    redis_client: fakeredis.aioredis.FakeRedis,
    device_id: str,
    capacity: int = 5,
    refill_rate: float = 1.0,
) -> DeviceTokenBucket:
    return DeviceTokenBucket(
        redis_client=redis_client,
        device_id=device_id,
        capacity=capacity,
        refill_rate=refill_rate,
    )


def _make_breaker(
    redis_client: fakeredis.aioredis.FakeRedis,
    device_id: str,
    error_rate_threshold: float = 0.5,
    p95_latency_threshold_ms: float = 2000.0,
    consecutive_timeout_threshold: int = 3,
    window_seconds: int = 60,
    half_open_probe_ttl: int = 10,
    open_state_ttl: int = 30,
) -> DeviceCircuitBreaker:
    return DeviceCircuitBreaker(
        redis_client=redis_client,
        device_id=device_id,
        error_rate_threshold=error_rate_threshold,
        p95_latency_threshold_ms=p95_latency_threshold_ms,
        consecutive_timeout_threshold=consecutive_timeout_threshold,
        window_seconds=window_seconds,
        half_open_probe_ttl=half_open_probe_ttl,
        open_state_ttl=open_state_ttl,
    )


# ===========================================================================
# TestDeviceSemaphore
# ===========================================================================


class TestDeviceSemaphore:
    """Tests for DeviceSemaphore."""

    @pytest.mark.asyncio
    async def test_acquire_single_slot_succeeds(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        sem = _make_semaphore(fake_redis, device_id, max_slots=1)
        acquired = await sem.acquire("worker-1", timeout_seconds=1.0)
        assert acquired is True

    @pytest.mark.asyncio
    async def test_acquire_up_to_max_slots_all_succeed(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        sem = _make_semaphore(fake_redis, device_id, max_slots=3)
        results = [
            await sem.acquire(f"worker-{i}", timeout_seconds=1.0)
            for i in range(3)
        ]
        assert all(results)

    @pytest.mark.asyncio
    async def test_acquire_beyond_max_slots_fails(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        sem = _make_semaphore(fake_redis, device_id, max_slots=2)
        await sem.acquire("worker-1", timeout_seconds=1.0)
        await sem.acquire("worker-2", timeout_seconds=1.0)
        # Third acquire should not get a slot (timeout quickly).
        acquired = await sem.acquire("worker-3", timeout_seconds=0.05)
        assert acquired is False

    @pytest.mark.asyncio
    async def test_release_frees_a_slot(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        sem = _make_semaphore(fake_redis, device_id, max_slots=1)
        await sem.acquire("worker-1", timeout_seconds=1.0)
        # All slots occupied; verify next acquire fails.
        blocked = await sem.acquire("worker-2", timeout_seconds=0.05)
        assert blocked is False

        # Release slot-1; now slot-2 should succeed.
        await sem.release("worker-1")
        acquired = await sem.acquire("worker-2", timeout_seconds=1.0)
        assert acquired is True

    @pytest.mark.asyncio
    async def test_release_is_idempotent(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        sem = _make_semaphore(fake_redis, device_id, max_slots=2)
        await sem.acquire("worker-1", timeout_seconds=1.0)
        # Double release must not raise.
        await sem.release("worker-1")
        await sem.release("worker-1")  # second call is a no-op

    @pytest.mark.asyncio
    async def test_release_nonexistent_worker_is_safe(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        sem = _make_semaphore(fake_redis, device_id, max_slots=2)
        # Releasing a worker that never acquired must not raise.
        await sem.release("ghost-worker")

    @pytest.mark.asyncio
    async def test_expired_slot_not_counted(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        """A semaphore key with a very short TTL expires; next acquire succeeds."""
        sem = _make_semaphore(fake_redis, device_id, max_slots=1, slot_ttl=1)
        await sem.acquire("worker-1", timeout_seconds=1.0)

        # Manually expire the key to simulate TTL expiry.
        await fake_redis.delete(f"sem:{device_id}")

        # The key is gone; a new acquire should succeed immediately.
        acquired = await sem.acquire("worker-2", timeout_seconds=1.0)
        assert acquired is True

    @pytest.mark.asyncio
    async def test_renew_returns_true_when_slot_exists(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        sem = _make_semaphore(fake_redis, device_id, max_slots=1)
        await sem.acquire("worker-1", timeout_seconds=1.0)
        result = await sem.renew("worker-1", new_ttl=60)
        assert result is True

    @pytest.mark.asyncio
    async def test_renew_returns_false_when_slot_reclaimed(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        sem = _make_semaphore(fake_redis, device_id, max_slots=1)
        await sem.acquire("worker-1", timeout_seconds=1.0)
        # Expire the key to simulate TTL-based reclaim.
        await fake_redis.delete(f"sem:{device_id}")
        result = await sem.renew("worker-1", new_ttl=60)
        assert result is False

    @pytest.mark.asyncio
    async def test_slot_context_manager_acquires_and_releases(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        sem = _make_semaphore(fake_redis, device_id, max_slots=1)
        async with sem.slot("worker-ctx", timeout_seconds=1.0) as acquired:
            assert acquired is True
        # After context exit, the slot should be released.
        acquired_again = await sem.acquire("other-worker", timeout_seconds=1.0)
        assert acquired_again is True

    @pytest.mark.asyncio
    async def test_slot_context_manager_releases_on_exception(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        sem = _make_semaphore(fake_redis, device_id, max_slots=1)
        with pytest.raises(RuntimeError):
            async with sem.slot("worker-exc", timeout_seconds=1.0) as acquired:
                assert acquired is True
                raise RuntimeError("simulated failure")
        # Slot must be free after the exception.
        acquired = await sem.acquire("next-worker", timeout_seconds=1.0)
        assert acquired is True

    @pytest.mark.asyncio
    async def test_device_id_is_scoped_per_device(
        self, fake_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        """Slots for different devices are independent."""
        sem_a = _make_semaphore(fake_redis, "dev-a", max_slots=1)
        sem_b = _make_semaphore(fake_redis, "dev-b", max_slots=1)

        await sem_a.acquire("worker-1", timeout_seconds=1.0)
        # dev-a is full; dev-b should still be available.
        acquired_b = await sem_b.acquire("worker-1", timeout_seconds=1.0)
        assert acquired_b is True


# ===========================================================================
# TestDeviceTokenBucket
# ===========================================================================


class TestDeviceTokenBucket:
    """Tests for DeviceTokenBucket."""

    @pytest.mark.asyncio
    async def test_burst_up_to_capacity_succeeds(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        bucket = _make_bucket(fake_redis, device_id, capacity=5, refill_rate=1.0)
        results = [await bucket.consume() for _ in range(5)]
        assert all(results)

    @pytest.mark.asyncio
    async def test_reject_when_empty(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        bucket = _make_bucket(fake_redis, device_id, capacity=3, refill_rate=0.01)
        # Drain the bucket.
        for _ in range(3):
            await bucket.consume()
        # Next consume should be rejected.
        allowed = await bucket.consume()
        assert allowed is False

    @pytest.mark.asyncio
    async def test_tokens_refill_over_time(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        """
        Inject a past last_refill time to simulate elapsed time.

        We manipulate the bucket hash directly to set last_refill to a time
        sufficiently in the past, then verify consume() allows a request.
        """
        bucket = _make_bucket(fake_redis, device_id, capacity=5, refill_rate=2.0)
        key = f"bucket:{device_id}"

        # Drain all tokens.
        for _ in range(5):
            await bucket.consume()

        # Simulate 3 seconds passing: 3 * 2.0 = 6 tokens should refill
        # (capped at capacity=5).
        past_time = time.time() - 3.0
        await fake_redis.hset(key, mapping={"tokens": "0", "last_refill": str(past_time)})

        # Should now be allowed.
        allowed = await bucket.consume()
        assert allowed is True

    @pytest.mark.asyncio
    async def test_token_count_does_not_exceed_capacity(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        """
        Even with a large elapsed time the bucket should cap at capacity.
        """
        bucket = _make_bucket(fake_redis, device_id, capacity=3, refill_rate=100.0)
        key = f"bucket:{device_id}"

        # Set up a very old last_refill.
        past_time = time.time() - 1000.0
        await fake_redis.hset(
            key,
            mapping={"tokens": "0", "last_refill": str(past_time)},
        )

        # Consume all 3 (capacity) tokens; fourth should be rejected.
        results = [await bucket.consume() for _ in range(3)]
        assert all(results)
        rejected = await bucket.consume()
        assert rejected is False

    @pytest.mark.asyncio
    async def test_consume_multiple_tokens(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        bucket = _make_bucket(fake_redis, device_id, capacity=10, refill_rate=1.0)
        allowed = await bucket.consume(tokens=5)
        assert allowed is True
        # 5 remain; consuming 6 should fail.
        rejected = await bucket.consume(tokens=6)
        assert rejected is False

    @pytest.mark.asyncio
    async def test_device_id_scoped_independently(
        self, fake_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        bucket_a = _make_bucket(fake_redis, "dev-a", capacity=1, refill_rate=0.01)
        bucket_b = _make_bucket(fake_redis, "dev-b", capacity=1, refill_rate=0.01)

        await bucket_a.consume()  # drain dev-a
        # dev-b should still allow.
        allowed_b = await bucket_b.consume()
        assert allowed_b is True

    @pytest.mark.asyncio
    async def test_wait_and_consume_returns_true_when_tokens_available(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        bucket = _make_bucket(fake_redis, device_id, capacity=2, refill_rate=1.0)
        result = await bucket.wait_and_consume(timeout=1.0)
        assert result is True

    @pytest.mark.asyncio
    async def test_wait_and_consume_times_out_when_empty(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        bucket = _make_bucket(fake_redis, device_id, capacity=2, refill_rate=0.001)
        # Drain.
        await bucket.consume()
        await bucket.consume()
        # Should time out quickly.
        result = await bucket.wait_and_consume(timeout=0.1)
        assert result is False


# ===========================================================================
# TestDeviceCircuitBreaker
# ===========================================================================


class TestDeviceCircuitBreaker:
    """Tests for DeviceCircuitBreaker."""

    @pytest.mark.asyncio
    async def test_initial_state_is_closed(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        breaker = _make_breaker(fake_redis, device_id)
        state = await breaker.peek_state()
        assert state is BreakerState.CLOSED

    @pytest.mark.asyncio
    async def test_stays_closed_on_successes(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        breaker = _make_breaker(fake_redis, device_id, error_rate_threshold=0.5)
        for _ in range(10):
            await breaker.record_success(latency_ms=100.0)
        state = await breaker.peek_state()
        assert state is BreakerState.CLOSED

    @pytest.mark.asyncio
    async def test_trips_open_on_high_error_rate(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        breaker = _make_breaker(
            fake_redis,
            device_id,
            error_rate_threshold=0.4,  # trip above 40% errors
            open_state_ttl=30,
        )
        # 5 failures out of 5 = 100% error rate.
        for _ in range(5):
            await breaker.record_failure(latency_ms=200.0)
        state = await breaker.peek_state()
        assert state is BreakerState.OPEN

    @pytest.mark.asyncio
    async def test_trips_open_on_high_latency(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        breaker = _make_breaker(
            fake_redis,
            device_id,
            p95_latency_threshold_ms=500.0,
            open_state_ttl=30,
        )
        # Record 10 successes with latency well above threshold.
        for _ in range(10):
            await breaker.record_success(latency_ms=3000.0)
        state = await breaker.peek_state()
        assert state is BreakerState.OPEN

    @pytest.mark.asyncio
    async def test_half_open_probe_fires_once(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        """
        When the breaker is open, exactly one caller gets HALF_OPEN; the rest
        must receive OPEN until the probe key expires or is cleared.
        """
        breaker = _make_breaker(
            fake_redis,
            device_id,
            error_rate_threshold=0.4,
            open_state_ttl=60,
            half_open_probe_ttl=30,
        )
        # Trip the breaker.
        for _ in range(5):
            await breaker.record_failure(latency_ms=100.0)

        # First get_state call should return half_open (claims probe).
        state1 = await breaker.get_state()
        assert state1 is BreakerState.HALF_OPEN

        # Subsequent calls should return open (probe in flight).
        state2 = await breaker.get_state()
        assert state2 is BreakerState.OPEN

    @pytest.mark.asyncio
    async def test_reset_closes_breaker(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        breaker = _make_breaker(
            fake_redis,
            device_id,
            error_rate_threshold=0.4,
            open_state_ttl=60,
        )
        # Trip the breaker.
        for _ in range(5):
            await breaker.record_failure(latency_ms=100.0)
        assert await breaker.peek_state() is BreakerState.OPEN

        # Reset (simulating successful probe).
        await breaker.reset()
        assert await breaker.peek_state() is BreakerState.CLOSED

    @pytest.mark.asyncio
    async def test_release_probe_allows_another_probe(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        """
        After a failed probe: release_probe() clears the probe key so another
        probe can fire, but the breaker remains open.
        """
        breaker = _make_breaker(
            fake_redis,
            device_id,
            error_rate_threshold=0.4,
            open_state_ttl=60,
            half_open_probe_ttl=30,
        )
        for _ in range(5):
            await breaker.record_failure(latency_ms=100.0)

        # Claim the probe.
        state = await breaker.get_state()
        assert state is BreakerState.HALF_OPEN

        # Probe fails → release without resetting.
        await breaker.release_probe()

        # Now another probe is possible.
        state2 = await breaker.get_state()
        assert state2 is BreakerState.HALF_OPEN

        # Breaker is still open (not reset).
        assert await breaker.peek_state() is BreakerState.HALF_OPEN

    @pytest.mark.asyncio
    async def test_one_device_failing_does_not_affect_another(
        self, fake_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        """Per D-5: one failing device must not affect other devices."""
        breaker_a = _make_breaker(
            fake_redis,
            "dev-a",
            error_rate_threshold=0.4,
            open_state_ttl=60,
        )
        breaker_b = _make_breaker(
            fake_redis,
            "dev-b",
            error_rate_threshold=0.4,
            open_state_ttl=60,
        )
        # Trip dev-a.
        for _ in range(5):
            await breaker_a.record_failure(latency_ms=100.0)
        assert await breaker_a.peek_state() is BreakerState.OPEN

        # dev-b is unaffected.
        assert await breaker_b.peek_state() is BreakerState.CLOSED

    @pytest.mark.asyncio
    async def test_record_timeout_contributes_to_error_rate(
        self, fake_redis: fakeredis.aioredis.FakeRedis, device_id: str
    ) -> None:
        breaker = _make_breaker(
            fake_redis,
            device_id,
            error_rate_threshold=0.4,
            consecutive_timeout_threshold=3,
            window_seconds=60,
            open_state_ttl=30,
        )
        # Timeouts are counted as errors in the error_rate calc.
        for _ in range(5):
            await breaker.record_timeout()
        state = await breaker.peek_state()
        assert state is BreakerState.OPEN


# ===========================================================================
# TestRedisUnavailable
# ===========================================================================


class TestRedisUnavailable:
    """
    Verify that all coordination primitives raise RedisUnavailableError
    when Redis is unreachable, implementing the fail-closed behaviour (D-4).
    """

    @pytest.fixture()
    def unavailable_redis(self) -> MagicMock:
        """A mock async Redis client that raises ConnectionError on every call."""
        from redis.exceptions import ConnectionError as RedisConnErr

        mock = MagicMock()
        mock.eval = AsyncMock(side_effect=RedisConnErr("Connection refused"))
        mock.exists = AsyncMock(side_effect=RedisConnErr("Connection refused"))
        mock.delete = AsyncMock(side_effect=RedisConnErr("Connection refused"))
        mock.hset = AsyncMock(side_effect=RedisConnErr("Connection refused"))
        return mock

    @pytest.mark.asyncio
    async def test_semaphore_acquire_raises_on_unavailable_redis(
        self, unavailable_redis: MagicMock, device_id: str
    ) -> None:
        sem = _make_semaphore(unavailable_redis, device_id)
        with pytest.raises(RedisUnavailableError):
            await sem.acquire("worker-1", timeout_seconds=1.0)

    @pytest.mark.asyncio
    async def test_semaphore_release_raises_on_unavailable_redis(
        self, unavailable_redis: MagicMock, device_id: str
    ) -> None:
        sem = _make_semaphore(unavailable_redis, device_id)
        with pytest.raises(RedisUnavailableError):
            await sem.release("worker-1")

    @pytest.mark.asyncio
    async def test_semaphore_renew_raises_on_unavailable_redis(
        self, unavailable_redis: MagicMock, device_id: str
    ) -> None:
        sem = _make_semaphore(unavailable_redis, device_id)
        with pytest.raises(RedisUnavailableError):
            await sem.renew("worker-1")

    @pytest.mark.asyncio
    async def test_token_bucket_consume_raises_on_unavailable_redis(
        self, unavailable_redis: MagicMock, device_id: str
    ) -> None:
        bucket = _make_bucket(unavailable_redis, device_id)
        with pytest.raises(RedisUnavailableError):
            await bucket.consume()

    @pytest.mark.asyncio
    async def test_breaker_record_success_raises_on_unavailable_redis(
        self, unavailable_redis: MagicMock, device_id: str
    ) -> None:
        breaker = _make_breaker(unavailable_redis, device_id)
        with pytest.raises(RedisUnavailableError):
            await breaker.record_success(latency_ms=100.0)

    @pytest.mark.asyncio
    async def test_breaker_peek_state_raises_on_unavailable_redis(
        self, unavailable_redis: MagicMock, device_id: str
    ) -> None:
        breaker = _make_breaker(unavailable_redis, device_id)
        with pytest.raises(RedisUnavailableError):
            await breaker.peek_state()

    @pytest.mark.asyncio
    async def test_breaker_get_state_raises_on_unavailable_redis(
        self, unavailable_redis: MagicMock, device_id: str
    ) -> None:
        breaker = _make_breaker(unavailable_redis, device_id)
        with pytest.raises(RedisUnavailableError):
            await breaker.get_state()

    @pytest.mark.asyncio
    async def test_breaker_reset_raises_on_unavailable_redis(
        self, unavailable_redis: MagicMock, device_id: str
    ) -> None:
        breaker = _make_breaker(unavailable_redis, device_id)
        with pytest.raises(RedisUnavailableError):
            await breaker.reset()


# ===========================================================================
# TestRedisOOM
# ===========================================================================


class TestRedisOOM:
    """
    Verify that writes raise RedisOOMError when Redis returns an OOM response.
    This happens with maxmemory-policy=noeviction (mandatory per D-3).
    """

    @pytest.fixture()
    def oom_redis(self) -> MagicMock:
        """A mock async Redis client that raises OOM ResponseError on every call."""
        from redis.exceptions import ResponseError

        mock = MagicMock()
        mock.eval = AsyncMock(
            side_effect=ResponseError("OOM command not allowed when used memory > 'maxmemory'")
        )
        mock.exists = AsyncMock(
            side_effect=ResponseError("OOM command not allowed when used memory > 'maxmemory'")
        )
        mock.delete = AsyncMock(
            side_effect=ResponseError("OOM command not allowed when used memory > 'maxmemory'")
        )
        return mock

    @pytest.mark.asyncio
    async def test_semaphore_acquire_raises_redis_oom_error(
        self, oom_redis: MagicMock, device_id: str
    ) -> None:
        sem = _make_semaphore(oom_redis, device_id)
        with pytest.raises(RedisOOMError):
            await sem.acquire("worker-1", timeout_seconds=1.0)

    @pytest.mark.asyncio
    async def test_token_bucket_consume_raises_redis_oom_error(
        self, oom_redis: MagicMock, device_id: str
    ) -> None:
        bucket = _make_bucket(oom_redis, device_id)
        with pytest.raises(RedisOOMError):
            await bucket.consume()

    @pytest.mark.asyncio
    async def test_breaker_record_raises_redis_oom_error(
        self, oom_redis: MagicMock, device_id: str
    ) -> None:
        breaker = _make_breaker(oom_redis, device_id)
        with pytest.raises(RedisOOMError):
            await breaker.record_failure(latency_ms=100.0)
