"""
Integration tests — T-8.1: Concurrency correctness.

These tests exercise the full request path against real Redis (via testcontainers
or a local Docker Redis) and a real SQLite/MSSQL test database.

Requirements:
  - Redis running at TEST_REDIS_URL (env var, default redis://localhost:6379/1)
  - MSSQL or SQLite test DB at TEST_DB_CONNECTION_STRING

All P-n values are set to safe test values here — never copy to production.
"""
import asyncio
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

# ── Markers: skip if live infra not available ──────────────────────────────────

pytestmark = [
    pytest.mark.integration,
    pytest.mark.slow,
]


@pytest.fixture(scope="module")
def redis_client():
    """Real Redis client for integration tests."""
    import os
    import redis.asyncio as aioredis

    url = os.getenv("TEST_REDIS_URL", "redis://localhost:6379/1")
    client = aioredis.from_url(url, decode_responses=True)
    yield client
    # Cleanup: flush test DB
    asyncio.get_event_loop().run_until_complete(client.flushdb())
    asyncio.get_event_loop().run_until_complete(client.aclose())


@pytest.fixture(scope="module")
def db_conn():
    """Real DB connection for integration tests."""
    import os
    import pyodbc

    cs = os.getenv(
        "TEST_DB_CONNECTION_STRING",
        "Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=gtm_test;Trusted_Connection=yes;",
    )
    conn = pyodbc.connect(cs)
    yield conn
    conn.close()


# ── T-8.1 test 1: 50 concurrent identical POSTs → exactly 1 succeeds ────────

@pytest.mark.asyncio
async def test_concurrent_identical_posts_exactly_one_inserted(redis_client, db_conn):
    """
    50 simultaneous requests for the same FQDN on the same device.
    Only 1 should be created (RECEIVED/QUEUED); the other 49 should get
    the existing request_id back (idempotent replay OR 409).
    """
    from app.db.claim import atomic_insert_and_claim
    from app.api.idempotency import compute_idempotency_key
    from app.domain.states import Status
    import datetime

    fqdn = f"test-concurrent-{uuid.uuid4()}.example.com"
    action = "create"
    payload = {"wideip_type": "a", "pools": []}
    ikey = compute_idempotency_key(action, fqdn, payload)

    def make_request_dict():
        return {
            "request_id": uuid.uuid4(),
            "idempotency_key": ikey,
            "action": action,
            "wip_fqdn": fqdn,
            "target_device": "test-device",
            "payload_hash": "abc123",
            "payload_json": "{}",
            "status": Status.RECEIVED.value,
            "created_at": datetime.datetime.now(datetime.timezone.utc),
            "updated_at": datetime.datetime.now(datetime.timezone.utc),
        }

    results = []
    with ThreadPoolExecutor(max_workers=50) as pool:
        futures = [
            pool.submit(atomic_insert_and_claim, db_conn, make_request_dict())
            for _ in range(50)
        ]
        for f in futures:
            results.append(f.result())

    created_count = sum(1 for created, _ in results if created)
    assert created_count == 1, f"Expected exactly 1 row created, got {created_count}"

    # All 49 that lost the race should have returned the same existing row
    existing_ids = {str(row.request_id) for created, row in results if not created}
    winner_id = str(next(row for created, row in results if created).request_id)
    assert all(eid == winner_id for eid in existing_ids), \
        "All losers must return the winner's request_id"


# ── T-8.1 test 2: Worker killed mid-workflow is reclaimed ─────────────────────

@pytest.mark.asyncio
async def test_killed_worker_is_reclaimed(redis_client, db_conn):
    """
    A RUNNING row with a stale heartbeat (beyond P-6 threshold) must be
    reclaimed by the sweeper and re-enqueued.
    A RUNNING row with a fresh heartbeat must NOT be reclaimed.
    """
    from app.recovery.reclaim import WorkerReclaimer
    from app.db.repositories import RequestRepository
    from app.domain.states import Status
    import datetime

    repo = RequestRepository(db_conn)

    # Insert a RUNNING row with a stale heartbeat
    stale_id = uuid.uuid4()
    repo.insert({
        "request_id": stale_id,
        "idempotency_key": str(uuid.uuid4()),
        "action": "create",
        "wip_fqdn": f"stale-{stale_id}.example.com",
        "target_device": "test-device",
        "payload_hash": "abc",
        "payload_json": "{}",
        "status": Status.RUNNING.value,
        "created_at": datetime.datetime.now(datetime.timezone.utc),
        "updated_at": datetime.datetime.now(datetime.timezone.utc),
        "worker_id": "dead-worker",
        "pod_id": "dead-pod",
        "started_at": datetime.datetime.now(datetime.timezone.utc),
        # Heartbeat set 10 minutes ago (stale beyond P-6 = 3×P-5)
        "last_heartbeat_at": datetime.datetime(2000, 1, 1),
    })

    # Insert a RUNNING row with a FRESH heartbeat — must NOT be reclaimed
    fresh_id = uuid.uuid4()
    repo.insert({
        "request_id": fresh_id,
        "idempotency_key": str(uuid.uuid4()),
        "action": "create",
        "wip_fqdn": f"fresh-{fresh_id}.example.com",
        "target_device": "test-device",
        "payload_hash": "abc",
        "payload_json": "{}",
        "status": Status.RUNNING.value,
        "created_at": datetime.datetime.now(datetime.timezone.utc),
        "updated_at": datetime.datetime.now(datetime.timezone.utc),
        "worker_id": "live-worker",
        "pod_id": "live-pod",
        "started_at": datetime.datetime.now(datetime.timezone.utc),
        "last_heartbeat_at": datetime.datetime.now(datetime.timezone.utc),
    })

    celery_mock = type("CeleryMock", (), {"send_task": lambda *a, **k: None})()
    reclaimer = WorkerReclaimer(
        db_conn_factory=lambda: db_conn,
        celery_app=celery_mock,
        heartbeat_stale_threshold=90,  # 90 seconds — test value only
    )
    result = await reclaimer.run()

    stale_row = repo.get_by_id(stale_id)
    fresh_row = repo.get_by_id(fresh_id)

    assert stale_row.status == Status.QUEUED.value, "Stale worker must be reclaimed to QUEUED"
    assert fresh_row.status == Status.RUNNING.value, "Live worker must NOT be reclaimed"
    assert result["reclaimed_running"] >= 1


# ── T-8.2 test: Retry storm is absorbed at admission ─────────────────────────

@pytest.mark.asyncio
async def test_retry_storm_absorbed_at_admission(redis_client):
    """
    A burst of 200 requests against a set of 5 FQDNs should be absorbed
    at admission (queue depth limit), consuming no worker capacity.
    """
    # TODO: implement once P-7 and P-8 values are known from T-0.6
    pytest.skip("Blocked on P-7 and P-8 values from T-0.6/T-0.7")


# ── T-8.5 test: Rollback never destroys pre-existing objects ─────────────────

@pytest.mark.asyncio
async def test_rollback_restores_not_deletes_preexisting_wideip():
    """
    Scenario: PUT against a WideIP that already exists on F5.
    Step 4 (WideIP update) succeeds; Step 5 (CNAME) fails.
    Rollback must restore the WideIP to its prior state, never delete it.
    """
    # TODO: implement with respx mocks for F5 and Infoblox
    # Key assertions:
    #   - F5 DELETE /wideip/ is never called
    #   - F5 PUT /wideip/ is called with the original (pre_state) config
    pytest.skip("Blocked: requires respx mock setup for full workflow")
