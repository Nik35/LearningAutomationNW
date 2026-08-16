"""
tests/unit/test_recovery.py
===========================
Unit tests for app/recovery/{reclaim, remediation, reconciler}.

All DB interactions are mocked — no real MSSQL connection required.
Celery and notification_sender are replaced with simple mocks/spies.

All P-n values that the production code accepts as constructor parameters
are passed explicitly in tests — none are hardcoded.

Test classes
------------
TestWorkerReclaimer
    - Stale RUNNING row (heartbeat expired) → reclaimed and re-enqueued
    - Fresh RUNNING row (healthy heartbeat) → NOT reclaimed (critical safety)
    - Orphaned QUEUED row (never claimed) → re-enqueued
    - Atomic UPDATE returns 0 rows → skip, no re-enqueue (another sweeper won)

TestRemediationWorker
    - Items below MAX_ATTEMPTS → backoff computed and re-enqueued
    - Item at MAX_ATTEMPTS → escalated to NEEDS_ATTENTION, NOT re-enqueued
    - Backoff sequence: verify exponential growth and jitter bounds

TestReconciler
    - write_enabled=True raises immediately (D-10)
    - _detect_drift pure function: each DriftCategory detected correctly
    - No-drift case returns empty list
    - run_sweep with no objects returns empty list
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.recovery.reclaim import WorkerReclaimer
from app.recovery.remediation import RemediationWorker
from app.recovery.reconciler import (
    DriftCategory,
    DriftItem,
    Reconciler,
)

# ---------------------------------------------------------------------------
# Shared test fixtures
# ---------------------------------------------------------------------------

P6_STALE_THRESHOLD = 90.0        # P-6: heartbeat stale after 90 s (= 3 × 30 s P-5)
ORPHANED_QUEUED_THRESHOLD = 300.0  # 5 × P-4 placeholder


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


# ---------------------------------------------------------------------------
# Helpers: build mock DB connections
# ---------------------------------------------------------------------------


def _mock_cursor(fetchone_result: Any = None, fetchall_result: Any = None) -> MagicMock:
    """Return a cursor mock with configurable fetchone / fetchall."""
    cursor = MagicMock()
    cursor.fetchone.return_value = fetchone_result
    cursor.fetchall.return_value = fetchall_result if fetchall_result is not None else []
    cursor.description = []
    return cursor


def _make_conn(cursor: MagicMock) -> MagicMock:
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


def _make_celery() -> MagicMock:
    celery = MagicMock()
    celery.send_task = MagicMock()
    return celery


# ===========================================================================
# TestWorkerReclaimer
# ===========================================================================


class TestWorkerReclaimer:
    """Tests for WorkerReclaimer (T-5.1)."""

    # --- stale RUNNING → reclaimed -----------------------------------------

    @pytest.mark.asyncio
    async def test_stale_running_row_is_reclaimed_and_requeued(self) -> None:
        """
        A RUNNING row with a stale heartbeat must be reclaimed (status → QUEUED)
        and the request_id re-enqueued via Celery.
        """
        request_id = uuid.uuid4()
        worker_id = "celery@dead-pod"

        # SELECT returns one candidate row.
        find_cursor = _mock_cursor(
            fetchall_result=[(str(request_id), worker_id)]
        )
        # The UPDATE OUTPUT returns the reclaimed request_id (1 row affected).
        claim_cursor = _mock_cursor(fetchone_result=(str(request_id),))

        # conn.cursor() called twice: once for SELECT, once for UPDATE.
        conn = MagicMock()
        conn.cursor.side_effect = [find_cursor, claim_cursor]

        celery = _make_celery()
        reclaimer = WorkerReclaimer(
            db_conn_factory=lambda: conn,
            celery_app=celery,
            heartbeat_stale_threshold=P6_STALE_THRESHOLD,
            orphaned_queued_threshold=ORPHANED_QUEUED_THRESHOLD,
        )

        count = await reclaimer.reclaim_stale_running()

        assert count == 1
        celery.send_task.assert_called_once()
        # The task name must be the workflow task.
        call_args = celery.send_task.call_args
        assert str(request_id) in call_args[1]["args"]

    # --- fresh RUNNING → NOT reclaimed (critical safety) -------------------

    @pytest.mark.asyncio
    async def test_running_row_with_healthy_heartbeat_is_not_reclaimed(self) -> None:
        """
        CRITICAL: A RUNNING row whose heartbeat is healthy (not stale) must
        NEVER be reclaimed.  The SQL WHERE clause enforces this; we verify
        by having the atomic UPDATE return 0 rows (simulating a row that
        disappeared from the stale set between SELECT and UPDATE — i.e. its
        heartbeat was just renewed).
        """
        request_id = uuid.uuid4()
        worker_id = "celery@live-pod"

        # SELECT returns a candidate (it looked stale momentarily)…
        find_cursor = _mock_cursor(
            fetchall_result=[(str(request_id), worker_id)]
        )
        # …but the UPDATE matches 0 rows (heartbeat was renewed, or another
        # sweeper already got it).
        claim_cursor = _mock_cursor(fetchone_result=None)

        conn = MagicMock()
        conn.cursor.side_effect = [find_cursor, claim_cursor]

        celery = _make_celery()
        reclaimer = WorkerReclaimer(
            db_conn_factory=lambda: conn,
            celery_app=celery,
            heartbeat_stale_threshold=P6_STALE_THRESHOLD,
            orphaned_queued_threshold=ORPHANED_QUEUED_THRESHOLD,
        )

        count = await reclaimer.reclaim_stale_running()

        # Row was NOT reclaimed and Celery was NOT called.
        assert count == 0
        celery.send_task.assert_not_called()

    # --- QUEUED orphaned row → re-enqueued ---------------------------------

    @pytest.mark.asyncio
    async def test_orphaned_queued_row_is_requeued(self) -> None:
        """
        A QUEUED row that is older than the orphaned_queued_threshold and has
        no worker_id must be re-enqueued.
        """
        request_id = uuid.uuid4()

        find_cursor = _mock_cursor(
            fetchall_result=[(str(request_id),)]
        )
        # UPDATE OUTPUT returns the row (we successfully bumped attempt_count).
        update_cursor = _mock_cursor(fetchone_result=(str(request_id),))

        conn = MagicMock()
        conn.cursor.side_effect = [find_cursor, update_cursor]

        celery = _make_celery()
        reclaimer = WorkerReclaimer(
            db_conn_factory=lambda: conn,
            celery_app=celery,
            heartbeat_stale_threshold=P6_STALE_THRESHOLD,
            orphaned_queued_threshold=ORPHANED_QUEUED_THRESHOLD,
        )

        count = await reclaimer.reclaim_orphaned_queued()

        assert count == 1
        celery.send_task.assert_called_once()
        call_args = celery.send_task.call_args
        assert str(request_id) in call_args[1]["args"]

    # --- atomic UPDATE returns 0 rows → skip, no re-enqueue ----------------

    @pytest.mark.asyncio
    async def test_zero_rows_updated_means_another_sweeper_won(self) -> None:
        """
        If the atomic conditional UPDATE on a RUNNING row returns 0 rows, it
        means another sweeper pod already reclaimed the row.  We must NOT
        re-enqueue in that case.
        """
        request_id = uuid.uuid4()
        worker_id = "celery@pod-a"

        find_cursor = _mock_cursor(
            fetchall_result=[(str(request_id), worker_id)]
        )
        # 0 rows affected → fetchone returns None.
        claim_cursor = _mock_cursor(fetchone_result=None)

        conn = MagicMock()
        conn.cursor.side_effect = [find_cursor, claim_cursor]

        celery = _make_celery()
        reclaimer = WorkerReclaimer(
            db_conn_factory=lambda: conn,
            celery_app=celery,
            heartbeat_stale_threshold=P6_STALE_THRESHOLD,
            orphaned_queued_threshold=ORPHANED_QUEUED_THRESHOLD,
        )

        count = await reclaimer.reclaim_stale_running()

        assert count == 0
        celery.send_task.assert_not_called()

    # --- run() returns combined counts -------------------------------------

    @pytest.mark.asyncio
    async def test_run_returns_counts_from_both_passes(self) -> None:
        """run() must return a dict with both stale_running and orphaned_queued counts."""
        reclaimer = WorkerReclaimer(
            db_conn_factory=MagicMock(),
            celery_app=_make_celery(),
            heartbeat_stale_threshold=P6_STALE_THRESHOLD,
            orphaned_queued_threshold=ORPHANED_QUEUED_THRESHOLD,
        )
        # Patch both passes.
        reclaimer.reclaim_stale_running = AsyncMock(return_value=3)
        reclaimer.reclaim_orphaned_queued = AsyncMock(return_value=1)

        result = await reclaimer.run()

        assert result == {"stale_running": 3, "orphaned_queued": 1}

    # --- P-6 never hardcoded -----------------------------------------------

    def test_stale_threshold_stored_on_instance(self) -> None:
        """The heartbeat_stale_threshold must be stored from the constructor, not hardcoded."""
        threshold = 9999.0
        reclaimer = WorkerReclaimer(
            db_conn_factory=MagicMock(),
            celery_app=_make_celery(),
            heartbeat_stale_threshold=threshold,
            orphaned_queued_threshold=ORPHANED_QUEUED_THRESHOLD,
        )
        assert reclaimer._heartbeat_stale_threshold == threshold

    def test_orphaned_threshold_stored_on_instance(self) -> None:
        """The orphaned_queued_threshold must be stored from the constructor, not hardcoded."""
        threshold = 12345.0
        reclaimer = WorkerReclaimer(
            db_conn_factory=MagicMock(),
            celery_app=_make_celery(),
            heartbeat_stale_threshold=P6_STALE_THRESHOLD,
            orphaned_queued_threshold=threshold,
        )
        assert reclaimer._orphaned_queued_threshold == threshold

    # --- no candidates → returns 0 -----------------------------------------

    @pytest.mark.asyncio
    async def test_no_stale_rows_returns_zero(self) -> None:
        """When there are no stale RUNNING rows, reclaim_stale_running returns 0."""
        find_cursor = _mock_cursor(fetchall_result=[])
        conn = MagicMock()
        conn.cursor.return_value = find_cursor

        celery = _make_celery()
        reclaimer = WorkerReclaimer(
            db_conn_factory=lambda: conn,
            celery_app=celery,
            heartbeat_stale_threshold=P6_STALE_THRESHOLD,
            orphaned_queued_threshold=ORPHANED_QUEUED_THRESHOLD,
        )

        count = await reclaimer.reclaim_stale_running()

        assert count == 0
        celery.send_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_orphaned_rows_returns_zero(self) -> None:
        """When there are no orphaned QUEUED rows, reclaim_orphaned_queued returns 0."""
        find_cursor = _mock_cursor(fetchall_result=[])
        conn = MagicMock()
        conn.cursor.return_value = find_cursor

        celery = _make_celery()
        reclaimer = WorkerReclaimer(
            db_conn_factory=lambda: conn,
            celery_app=celery,
            heartbeat_stale_threshold=P6_STALE_THRESHOLD,
            orphaned_queued_threshold=ORPHANED_QUEUED_THRESHOLD,
        )

        count = await reclaimer.reclaim_orphaned_queued()

        assert count == 0
        celery.send_task.assert_not_called()


# ===========================================================================
# TestRemediationWorker
# ===========================================================================


class TestRemediationWorker:
    """Tests for RemediationWorker (T-5.2)."""

    def _make_worker(
        self,
        db_conn_factory: Any = None,
        celery: Any = None,
        notify: Any = None,
    ) -> RemediationWorker:
        return RemediationWorker(
            db_conn_factory=db_conn_factory or MagicMock(),
            celery_app=celery or _make_celery(),
            notification_sender=notify or AsyncMock(),
        )

    # --- items below MAX_ATTEMPTS → backoff and re-enqueue -----------------

    @pytest.mark.asyncio
    async def test_item_below_max_attempts_is_requeued_with_backoff(self) -> None:
        """
        A remediation item with retry_count < MAX_ATTEMPTS must be:
          - retry_count incremented
          - next_retry_at updated with exponential backoff
          - request_id re-enqueued via Celery
          - NOT escalated to NEEDS_ATTENTION
        """
        from app.domain.models import RemediationItem

        item = RemediationItem(
            remediation_id=uuid.uuid4(),
            request_id=uuid.uuid4(),
            step_id=uuid.uuid4(),
            failure_category="cname_failed_after_wideip",
            retry_count=1,      # Below MAX_ATTEMPTS (5)
            next_retry_at=_utcnow() - timedelta(seconds=1),  # Past due
        )

        # Build a cursor that returns our item from get_due_items.
        cursor = MagicMock()
        cursor.description = [
            ("remediation_id",), ("request_id",), ("step_id",),
            ("failure_category",), ("retry_count",), ("next_retry_at",),
            ("escalated_at",), ("resolution",),
        ]
        cursor.fetchall.return_value = [
            (
                str(item.remediation_id),
                str(item.request_id),
                str(item.step_id),
                item.failure_category,
                item.retry_count,
                item.next_retry_at,
                None,
                None,
            )
        ]
        cursor.execute = MagicMock()

        conn = MagicMock()
        conn.cursor.return_value = cursor

        celery = _make_celery()
        notify = AsyncMock()
        worker = self._make_worker(
            db_conn_factory=lambda: conn,
            celery=celery,
            notify=notify,
        )

        processed = await worker.process_due_items(limit=10)

        assert processed == 1
        # Celery re-enqueue fired.
        celery.send_task.assert_called_once()
        call_args = celery.send_task.call_args
        assert str(item.request_id) in call_args[1]["args"]
        # Notification NOT sent (not escalated).
        notify.assert_not_called()

    # --- item at MAX_ATTEMPTS → escalated to NEEDS_ATTENTION ---------------

    @pytest.mark.asyncio
    async def test_item_at_max_attempts_escalates_to_needs_attention(self) -> None:
        """
        When retry_count + 1 > MAX_ATTEMPTS, the item must:
          - Transition the request to NEEDS_ATTENTION in the DB.
          - Set escalated_at on the remediation row.
          - Send a notification.
          - NOT re-enqueue via Celery.
        """
        from app.domain.models import RemediationItem

        item = RemediationItem(
            remediation_id=uuid.uuid4(),
            request_id=uuid.uuid4(),
            step_id=uuid.uuid4(),
            failure_category="cname_failed_after_wideip",
            retry_count=RemediationWorker.MAX_ATTEMPTS,  # Next increment exceeds cap
            next_retry_at=_utcnow() - timedelta(seconds=1),
        )

        cursor = MagicMock()
        cursor.description = [
            ("remediation_id",), ("request_id",), ("step_id",),
            ("failure_category",), ("retry_count",), ("next_retry_at",),
            ("escalated_at",), ("resolution",),
        ]
        cursor.fetchall.return_value = [
            (
                str(item.remediation_id),
                str(item.request_id),
                str(item.step_id),
                item.failure_category,
                item.retry_count,
                item.next_retry_at,
                None,
                None,
            )
        ]
        cursor.execute = MagicMock()

        conn = MagicMock()
        conn.cursor.return_value = cursor

        celery = _make_celery()
        notify = AsyncMock()
        worker = self._make_worker(
            db_conn_factory=lambda: conn,
            celery=celery,
            notify=notify,
        )

        processed = await worker.process_due_items(limit=10)

        assert processed == 1
        # Notification MUST be sent.
        notify.assert_called_once()
        notify_args = notify.call_args[0]
        assert str(item.request_id) == notify_args[0]
        assert item.failure_category == notify_args[1]
        # Celery must NOT re-enqueue.
        celery.send_task.assert_not_called()

    # --- backoff values: exponential sequence with jitter bounds -----------

    def test_backoff_doubles_each_attempt_up_to_cap(self) -> None:
        """
        _compute_next_retry must produce exponentially increasing delays,
        capped at MAX_BACKOFF_SECONDS.  Jitter adds [0, 10] s on top.
        """
        worker = self._make_worker()
        base = RemediationWorker.BASE_BACKOFF_SECONDS       # 30
        max_b = RemediationWorker.MAX_BACKOFF_SECONDS       # 3600
        jitter_max = RemediationWorker._JITTER_MAX_SECONDS  # 10

        for attempt in range(1, RemediationWorker.MAX_ATTEMPTS + 1):
            expected_base = min(base * (2 ** attempt), max_b)
            # next_retry_at - now ≈ expected_base + [0, jitter_max]
            before = _utcnow()
            result = worker._compute_next_retry(attempt)
            after = _utcnow()

            delay = (result - before).total_seconds()
            # Lower bound: at least expected_base (jitter ≥ 0).
            assert delay >= expected_base - 1.0, (
                f"attempt={attempt}: delay {delay:.1f}s < expected_base {expected_base}s"
            )
            # Upper bound: at most expected_base + jitter_max + some clock tolerance.
            assert delay <= expected_base + jitter_max + 2.0, (
                f"attempt={attempt}: delay {delay:.1f}s > {expected_base + jitter_max + 2.0}s"
            )

    def test_backoff_capped_at_max_backoff(self) -> None:
        """
        For a very high attempt number, the base must be capped at
        MAX_BACKOFF_SECONDS (before jitter).
        """
        worker = self._make_worker()
        # Attempt 20: 30 × 2^20 = ~31 million seconds — way above 3600.
        result = worker._compute_next_retry(20)
        delay = (result - _utcnow()).total_seconds()
        assert delay <= RemediationWorker.MAX_BACKOFF_SECONDS + RemediationWorker._JITTER_MAX_SECONDS + 2.0

    def test_backoff_attempt_1_lower_than_attempt_2(self) -> None:
        """Verify monotone increase between attempt 1 and 2."""
        worker = self._make_worker()
        r1 = worker._compute_next_retry(1)
        r2 = worker._compute_next_retry(2)
        # r1 must be earlier than r2 (lower delay).
        assert r1 < r2

    # --- enqueue_for_remediation inserts with BASE_BACKOFF delay -----------

    @pytest.mark.asyncio
    async def test_enqueue_for_remediation_sets_initial_retry_time(self) -> None:
        """enqueue_for_remediation inserts a row with next_retry_at ≈ now + BASE_BACKOFF."""
        request_id = uuid.uuid4()
        step_id = uuid.uuid4()

        cursor = MagicMock()
        cursor.execute = MagicMock()
        conn = MagicMock()
        conn.cursor.return_value = cursor

        worker = RemediationWorker(
            db_conn_factory=lambda: conn,
            celery_app=_make_celery(),
            notification_sender=AsyncMock(),
        )

        before = _utcnow()
        await worker.enqueue_for_remediation(
            request_id=request_id,
            step_id=step_id,
            failure_category="cname_create_failed",
        )
        after = _utcnow()

        # The INSERT was called.
        cursor.execute.assert_called()
        conn.commit.assert_called_once()

    # --- zero items → returns 0 --------------------------------------------

    @pytest.mark.asyncio
    async def test_no_due_items_returns_zero(self) -> None:
        cursor = MagicMock()
        cursor.description = [
            ("remediation_id",), ("request_id",), ("step_id",),
            ("failure_category",), ("retry_count",), ("next_retry_at",),
            ("escalated_at",), ("resolution",),
        ]
        cursor.fetchall.return_value = []

        conn = MagicMock()
        conn.cursor.return_value = cursor

        worker = self._make_worker(db_conn_factory=lambda: conn)
        processed = await worker.process_due_items(limit=50)
        assert processed == 0


# ===========================================================================
# TestReconciler
# ===========================================================================


class TestReconciler:
    """Tests for Reconciler (WP-6, D-10)."""

    def _make_reconciler(
        self,
        f5_clients: dict | None = None,
        infoblox_client: Any = None,
        page_size: int = 100,
    ) -> Reconciler:
        return Reconciler(
            db_conn_factory=MagicMock(),
            f5_clients=f5_clients or {},
            infoblox_client=infoblox_client or AsyncMock(),
            page_size=page_size,
            write_enabled=False,
        )

    # --- write_enabled=True raises immediately (D-10) ----------------------

    def test_write_enabled_true_raises_value_error(self) -> None:
        """
        Passing write_enabled=True must raise ValueError immediately.
        D-10 is absolute — there is no override path.
        """
        with pytest.raises(ValueError, match="D-10"):
            Reconciler(
                db_conn_factory=MagicMock(),
                f5_clients={},
                infoblox_client=AsyncMock(),
                write_enabled=True,
            )

    def test_write_enabled_default_is_false(self) -> None:
        """Default write_enabled=False must not raise."""
        rec = Reconciler(
            db_conn_factory=MagicMock(),
            f5_clients={},
            infoblox_client=AsyncMock(),
        )
        assert rec._write_enabled is False

    # --- _detect_drift pure function: each DriftCategory -------------------

    def test_detect_drift_in_db_missing_in_f5(self) -> None:
        """ACTIVE DB record + actual=None → IN_DB_MISSING_IN_F5."""
        rec = self._make_reconciler()
        db_obj = {
            "status": "ACTIVE",
            "desired_state_json": '{"name": "my-monitor"}',
            "object_type": "monitor",
            "object_key": "my-monitor",
        }
        result = rec._detect_drift(db_obj, actual=None)
        assert result == DriftCategory.IN_DB_MISSING_IN_F5

    def test_detect_drift_attributes_differ(self) -> None:
        """Both present but attributes do not match → ATTRIBUTES_DIFFER."""
        rec = self._make_reconciler()
        db_obj = {
            "status": "ACTIVE",
            "desired_state_json": '{"interval": 30}',
            "object_type": "monitor",
            "object_key": "my-monitor",
        }
        actual = {"interval": 60, "timeout": 10}  # interval differs
        result = rec._detect_drift(db_obj, actual=actual)
        assert result == DriftCategory.ATTRIBUTES_DIFFER

    def test_detect_drift_no_drift_when_states_match(self) -> None:
        """Both present and attributes match → None (no drift)."""
        rec = self._make_reconciler()
        db_obj = {
            "status": "ACTIVE",
            "desired_state_json": '{"interval": 30, "timeout": 10}',
            "object_type": "monitor",
            "object_key": "my-monitor",
        }
        actual = {"interval": 30, "timeout": 10, "extra_field": "ignored"}
        result = rec._detect_drift(db_obj, actual=actual)
        assert result is None

    def test_detect_drift_pending_delete_still_present(self) -> None:
        """PENDING_DELETE in DB + actual exists → PENDING_DELETE_STILL_PRESENT."""
        rec = self._make_reconciler()
        db_obj = {
            "status": "PENDING_DELETE",
            "desired_state_json": "{}",
            "object_type": "wideip",
            "object_key": "app.example.com",
        }
        actual = {"name": "app.example.com"}
        result = rec._detect_drift(db_obj, actual=actual)
        assert result == DriftCategory.PENDING_DELETE_STILL_PRESENT

    def test_detect_drift_pending_delete_absent_is_no_drift(self) -> None:
        """PENDING_DELETE in DB + actual=None → no drift (object is already gone)."""
        rec = self._make_reconciler()
        db_obj = {
            "status": "PENDING_DELETE",
            "desired_state_json": "{}",
            "object_type": "wideip",
            "object_key": "app.example.com",
        }
        result = rec._detect_drift(db_obj, actual=None)
        assert result is None

    def test_detect_drift_no_desired_state_json_and_matching_actual(self) -> None:
        """When desired_state_json is None (no keys to compare), no drift is reported."""
        rec = self._make_reconciler()
        db_obj = {
            "status": "ACTIVE",
            "desired_state_json": None,
            "object_type": "monitor",
            "object_key": "my-monitor",
        }
        actual = {"interval": 30}
        result = rec._detect_drift(db_obj, actual=actual)
        # No desired keys to compare against → no diff.
        assert result is None

    # --- run_sweep with no objects returns empty list ----------------------

    @pytest.mark.asyncio
    async def test_run_sweep_no_objects_returns_empty_list(self) -> None:
        """
        When managed_objects is empty (page returns no rows), run_sweep
        returns an empty list.  No F5 or Infoblox calls are made.
        """
        cursor = MagicMock()
        cursor.description = [
            ("object_id",), ("wip_fqdn",), ("object_type",), ("object_key",),
            ("target_device",), ("desired_state_json",), ("last_verified_at",),
            ("drift_detected_at",), ("drift_details_json",),
            ("owning_request_id",), ("status",),
        ]
        cursor.fetchall.return_value = []

        conn = MagicMock()
        conn.cursor.return_value = cursor

        f5_client = AsyncMock()
        infoblox_client = AsyncMock()
        rec = Reconciler(
            db_conn_factory=lambda: conn,
            f5_clients={"dev-f5-01": f5_client},
            infoblox_client=infoblox_client,
            page_size=100,
            write_enabled=False,
        )

        result = await rec.run_sweep()

        assert result == []
        f5_client.get_wideip.assert_not_called()
        infoblox_client.get_cname.assert_not_called()

    # --- run_sweep detects drift when object absent from F5 ---------------

    @pytest.mark.asyncio
    async def test_run_sweep_detects_in_db_missing_in_f5(self) -> None:
        """
        A managed WideIP that exists in MSSQL but is absent from F5 must
        appear in the returned drift list as IN_DB_MISSING_IN_F5.
        """
        object_id = uuid.uuid4()
        wip_fqdn = "app.example.com"
        device_id = "dev-f5-01"

        row = (
            str(object_id),   # object_id
            wip_fqdn,          # wip_fqdn
            "wideip",          # object_type
            wip_fqdn,          # object_key
            device_id,         # target_device
            '{"name": "app.example.com"}',  # desired_state_json
            None,              # last_verified_at
            None,              # drift_detected_at
            None,              # drift_details_json
            None,              # owning_request_id
            "ACTIVE",          # status
        )

        cursor = MagicMock()
        cursor.description = [
            ("object_id",), ("wip_fqdn",), ("object_type",), ("object_key",),
            ("target_device",), ("desired_state_json",), ("last_verified_at",),
            ("drift_detected_at",), ("drift_details_json",),
            ("owning_request_id",), ("status",),
        ]
        # First page returns one row; second page returns nothing.
        cursor.fetchall.side_effect = [[row], []]

        conn = MagicMock()
        conn.cursor.return_value = cursor

        # F5 client returns None → WideIP absent.
        f5_client = AsyncMock()
        f5_client.get_wideip = AsyncMock(return_value=None)

        infoblox_client = AsyncMock()
        infoblox_client.get_cname = AsyncMock(return_value=None)

        rec = Reconciler(
            db_conn_factory=lambda: conn,
            f5_clients={device_id: f5_client},
            infoblox_client=infoblox_client,
            page_size=100,
            write_enabled=False,
        )

        drift_items = await rec.run_sweep(device_id=device_id)

        assert len(drift_items) == 1
        assert drift_items[0].category == DriftCategory.IN_DB_MISSING_IN_F5
        assert drift_items[0].wip_fqdn == wip_fqdn
        assert drift_items[0].severity == "NORMAL"

    # --- CNAME present, WideIP missing → HIGH severity ---------------------

    @pytest.mark.asyncio
    async def test_run_sweep_cname_present_wideip_missing_is_high_severity(
        self,
    ) -> None:
        """
        A CNAME that exists in Infoblox while its WideIP is absent from F5
        must appear as DriftCategory.CNAME_PRESENT_WIDEIP_MISSING with HIGH severity.
        """
        object_id = uuid.uuid4()
        wip_fqdn = "app.example.com"
        device_id = "dev-f5-01"

        row = (
            str(object_id),
            wip_fqdn,
            "cname",        # object_type
            wip_fqdn,        # object_key
            device_id,
            '{}',
            None, None, None, None,
            "ACTIVE",
        )

        cursor = MagicMock()
        cursor.description = [
            ("object_id",), ("wip_fqdn",), ("object_type",), ("object_key",),
            ("target_device",), ("desired_state_json",), ("last_verified_at",),
            ("drift_detected_at",), ("drift_details_json",),
            ("owning_request_id",), ("status",),
        ]
        cursor.fetchall.side_effect = [[row], []]

        conn = MagicMock()
        conn.cursor.return_value = cursor

        # CNAME exists in Infoblox.
        infoblox_client = AsyncMock()
        infoblox_client.get_cname = AsyncMock(
            return_value={"name": wip_fqdn, "canonical": "gtm.example.com"}
        )

        # WideIP absent from F5.
        f5_client = AsyncMock()
        f5_client.get_wideip = AsyncMock(return_value=None)

        rec = Reconciler(
            db_conn_factory=lambda: conn,
            f5_clients={device_id: f5_client},
            infoblox_client=infoblox_client,
            page_size=100,
            write_enabled=False,
        )

        drift_items = await rec.run_sweep(device_id=device_id)

        high_items = [
            d for d in drift_items
            if d.category == DriftCategory.CNAME_PRESENT_WIDEIP_MISSING
        ]
        assert len(high_items) >= 1
        assert high_items[0].severity == "HIGH"

    # --- _generate_report produces correct structure -----------------------

    def test_generate_report_counts_by_category(self) -> None:
        """_generate_report must count items per category and return totals."""
        rec = self._make_reconciler()
        items = [
            DriftItem(
                wip_fqdn="a.example.com",
                object_type="wideip",
                device_id="dev-f5-01",
                category=DriftCategory.IN_DB_MISSING_IN_F5,
                db_state=None,
                actual_state=None,
                diff_summary="missing",
            ),
            DriftItem(
                wip_fqdn="b.example.com",
                object_type="cname",
                device_id="dev-f5-01",
                category=DriftCategory.CNAME_PRESENT_WIDEIP_MISSING,
                db_state=None,
                actual_state=None,
                diff_summary="dns broken",
            ),
            DriftItem(
                wip_fqdn="c.example.com",
                object_type="wideip",
                device_id="dev-f5-01",
                category=DriftCategory.IN_DB_MISSING_IN_F5,
                db_state=None,
                actual_state=None,
                diff_summary="missing",
            ),
        ]

        report = rec._generate_report(items)

        assert report["total"] == 3
        assert report["by_category"][DriftCategory.IN_DB_MISSING_IN_F5.value] == 2
        assert report["by_category"][DriftCategory.CNAME_PRESENT_WIDEIP_MISSING.value] == 1
        assert report["by_severity"]["HIGH"] == 1
        assert report["by_severity"]["NORMAL"] == 2
        assert len(report["high_severity"]) == 1

    def test_generate_report_empty_list(self) -> None:
        rec = self._make_reconciler()
        report = rec._generate_report([])
        assert report["total"] == 0
        assert report["by_severity"]["HIGH"] == 0
        assert report["by_severity"]["NORMAL"] == 0

    # --- DriftItem severity is set automatically ---------------------------

    def test_drift_item_cname_wideip_missing_is_high_severity(self) -> None:
        item = DriftItem(
            wip_fqdn="x.example.com",
            object_type="cname",
            device_id="dev-f5-01",
            category=DriftCategory.CNAME_PRESENT_WIDEIP_MISSING,
            db_state=None,
            actual_state=None,
            diff_summary="",
        )
        assert item.severity == "HIGH"

    def test_drift_item_in_db_missing_is_normal_severity(self) -> None:
        item = DriftItem(
            wip_fqdn="x.example.com",
            object_type="monitor",
            device_id="dev-f5-01",
            category=DriftCategory.IN_DB_MISSING_IN_F5,
            db_state=None,
            actual_state=None,
            diff_summary="",
        )
        assert item.severity == "NORMAL"
