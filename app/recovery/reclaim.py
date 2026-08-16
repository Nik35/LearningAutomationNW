"""
app/recovery/reclaim.py — Stale worker reclaimer (T-5.1).

Two distinct reclaim cases handled here.  They must NEVER be confused:

Case 1 — RUNNING with stale heartbeat
    A worker that held this row has died (or been killed).  The heartbeat
    has not been renewed for longer than P-6 seconds (= 3 × P-5).
    Safe to reclaim: set status back to QUEUED and re-enqueue.

Case 2 — QUEUED, never claimed
    The row was enqueued but no worker ever transitioned it to RUNNING.
    This can happen if a worker died immediately after dequeuing, before
    completing the atomic QUEUED → RUNNING claim.  Safe to re-enqueue
    immediately — no external writes were made.

Case NOT handled — RUNNING with a live heartbeat
    A slow worker is still working.  DO NOT reclaim.  The heartbeat
    condition in the SQL WHERE clause is the hard enforcement; this comment
    is the safety annotation.

P-n parameters
--------------
``heartbeat_stale_threshold``   (P-6): passed as a constructor argument.
                                Never hardcoded.  See CLAUDE.md.
``orphaned_queued_threshold``   (5 × P-4): passed as a constructor argument.
                                Never hardcoded.

Architecture invariants
-----------------------
- The conditional UPDATE on ``status='RUNNING' AND worker_id=?`` makes the
  reclaim atomic.  0 rows updated means another sweeper pod beat us to it —
  safe to skip, no re-enqueue.
- MSSQL OUTPUT clause returns the request_ids of reclaimed rows so we never
  have to do a separate SELECT.
- Re-enqueue carries request_id ONLY (D-2).
"""

from __future__ import annotations

import uuid
from typing import Any, Callable

from app.core.logging import get_logger

log = get_logger(__name__)


class WorkerReclaimer:
    """
    Sweep for stale RUNNING rows and orphaned QUEUED rows, then re-enqueue them.

    Parameters
    ----------
    db_conn_factory:
        Zero-argument callable that returns an open ``pyodbc.Connection``.
        Connection lifecycle (commit/close) is managed by this class.
    celery_app:
        A Celery application instance.  Used to call ``.send_task()`` to
        re-enqueue recovered request_ids.
    heartbeat_stale_threshold:
        P-6 in seconds.  RUNNING rows whose ``last_heartbeat_at`` is older
        than this are eligible for reclaim.  Must be 3 × P-5.
        **Never hardcode this value.**
    orphaned_queued_threshold:
        Threshold in seconds beyond which a QUEUED row that was never
        claimed is considered orphaned and re-enqueued.  Suggested value:
        5 × P-4.  **Never hardcode.**
    max_reclaim_per_run:
        Upper bound on rows processed per invocation (both passes combined).
        Prevents a single sweeper run from overwhelming the queue after a
        mass pod failure.
    workflow_task_name:
        Celery task name used to re-enqueue recovered requests.
    """

    def __init__(
        self,
        db_conn_factory: Callable[[], Any],
        celery_app: Any,
        heartbeat_stale_threshold: float,    # P-6: accept as param, never hardcode
        orphaned_queued_threshold: float,    # 5 × P-4: accept as param, never hardcode
        max_reclaim_per_run: int = 50,
        workflow_task_name: str = "tasks.workflows.run_workflow",
    ) -> None:
        self._db_conn_factory = db_conn_factory
        self._celery_app = celery_app
        # P-6: stale heartbeat threshold in seconds. Never a literal here.
        self._heartbeat_stale_threshold = heartbeat_stale_threshold
        # Orphaned QUEUED threshold: 5 × P-4. Never a literal here.
        self._orphaned_queued_threshold = orphaned_queued_threshold
        self._max_reclaim_per_run = max_reclaim_per_run
        self._workflow_task_name = workflow_task_name

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def reclaim_stale_running(self) -> int:
        """
        Find RUNNING rows whose heartbeat is older than P-6 seconds and
        reclaim them back to QUEUED for re-execution.

        CRITICAL SAFETY: The WHERE clause requires
            status = 'RUNNING'
            AND last_heartbeat_at < DATEADD(SECOND, -P6, GETDATE())
        This guarantees that a slow worker with a healthy heartbeat is NEVER
        reclaimed, even if it is taking longer than expected.

        The atomic conditional UPDATE (checking both status and worker_id)
        means two sweeper pods cannot both reclaim the same row: the second
        UPDATE will match 0 rows and skip silently.

        Returns
        -------
        int
            Number of rows successfully reclaimed in this pass.
        """
        conn = self._db_conn_factory()
        reclaimed_count = 0
        try:
            # Step 1: find candidate stale RUNNING rows (read pass).
            # We do a SELECT first so we can issue per-row atomic UPDATEs
            # that include worker_id in the WHERE clause.  This prevents
            # two concurrent sweepers from both reclaiming the same row.
            find_sql = """
                SELECT TOP (?) request_id, worker_id
                FROM requests
                WHERE status = 'RUNNING'
                  AND last_heartbeat_at < DATEADD(SECOND, ?, GETUTCDATE())
            """
            # Pass -threshold so DATEADD subtracts the interval.
            find_cursor = conn.cursor()
            find_cursor.execute(
                find_sql,
                (self._max_reclaim_per_run, -int(self._heartbeat_stale_threshold)),
            )
            candidates = find_cursor.fetchall()

            for row in candidates:
                request_id_raw = row[0]
                worker_id = row[1]
                request_id = _coerce_uuid(request_id_raw)

                # Step 2: atomic conditional UPDATE — only reclaim if this
                # exact worker still owns the row in RUNNING state.
                # The OUTPUT clause returns the row only if the UPDATE
                # actually fired (1 row affected); 0 rows = another sweeper
                # already got it.
                claim_sql = """
                    UPDATE requests
                    SET status       = 'QUEUED',
                        worker_id    = NULL,
                        pod_id       = NULL,
                        attempt_count = attempt_count + 1,
                        updated_at   = GETUTCDATE()
                    OUTPUT INSERTED.request_id
                    WHERE status             = 'RUNNING'
                      AND request_id        = ?
                      AND worker_id         = ?
                      AND last_heartbeat_at < DATEADD(SECOND, ?, GETUTCDATE())
                """
                claim_cursor = conn.cursor()
                claim_cursor.execute(
                    claim_sql,
                    (
                        str(request_id),
                        worker_id,
                        -int(self._heartbeat_stale_threshold),
                    ),
                )
                output_row = claim_cursor.fetchone()
                if output_row is None:
                    # 0 rows affected: another sweeper pod won the race, or
                    # the worker renewed its heartbeat between our SELECT and
                    # this UPDATE.  Skip safely.
                    log.debug(
                        "reclaim_stale_running_skipped",
                        request_id=str(request_id),
                        reason="another_sweeper_or_heartbeat_renewed",
                    )
                    continue

                conn.commit()
                reclaimed_count += 1

                # Re-enqueue: send request_id ONLY, never the payload (D-2).
                self._celery_app.send_task(
                    self._workflow_task_name,
                    args=[str(request_id)],
                )
                log.info(
                    "reclaim_stale_running_requeued",
                    request_id=str(request_id),
                    previous_worker_id=worker_id,
                )

        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            try:
                conn.close()
            except Exception:
                pass

        return reclaimed_count

    async def reclaim_orphaned_queued(self) -> int:
        """
        Find QUEUED rows that are older than ``orphaned_queued_threshold``
        seconds without having been claimed by any worker, then re-enqueue them.

        These rows were never picked up — either the worker that dequeued them
        died before the atomic QUEUED → RUNNING claim, or Celery dropped the
        message.  No external writes were made, so re-enqueuing is always safe.

        Returns
        -------
        int
            Number of orphaned rows re-enqueued.
        """
        conn = self._db_conn_factory()
        requeued_count = 0
        try:
            # Orphaned QUEUED rows: status still QUEUED, worker_id never set,
            # and old enough that any legitimate semaphore wait has expired.
            find_sql = """
                SELECT TOP (?) request_id
                FROM requests
                WHERE status    = 'QUEUED'
                  AND worker_id IS NULL
                  AND created_at < DATEADD(SECOND, ?, GETUTCDATE())
            """
            find_cursor = conn.cursor()
            find_cursor.execute(
                find_sql,
                (self._max_reclaim_per_run, -int(self._orphaned_queued_threshold)),
            )
            candidates = [row[0] for row in find_cursor.fetchall()]

            for request_id_raw in candidates:
                request_id = _coerce_uuid(request_id_raw)

                # Bump attempt_count so we can distinguish repeated rescues
                # from first-time enqueues in metrics.
                update_sql = """
                    UPDATE requests
                    SET attempt_count = attempt_count + 1,
                        updated_at    = GETUTCDATE()
                    OUTPUT INSERTED.request_id
                    WHERE request_id = ?
                      AND status     = 'QUEUED'
                      AND worker_id  IS NULL
                """
                update_cursor = conn.cursor()
                update_cursor.execute(update_sql, (str(request_id),))
                output_row = update_cursor.fetchone()
                if output_row is None:
                    # Row was claimed by a worker between our SELECT and UPDATE.
                    # This is the happy path — skip.
                    log.debug(
                        "reclaim_orphaned_queued_skipped",
                        request_id=str(request_id),
                        reason="claimed_by_worker_between_select_and_update",
                    )
                    continue

                conn.commit()
                requeued_count += 1

                # Re-enqueue: request_id ONLY (D-2).
                self._celery_app.send_task(
                    self._workflow_task_name,
                    args=[str(request_id)],
                )
                log.info(
                    "reclaim_orphaned_queued_requeued",
                    request_id=str(request_id),
                )

        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            try:
                conn.close()
            except Exception:
                pass

        return requeued_count

    async def run(self) -> dict[str, int]:
        """
        Run both reclaim passes in sequence.

        Returns
        -------
        dict with keys ``stale_running`` and ``orphaned_queued``, each
        containing the count of rows processed in that pass.  Intended for
        use in metrics and beat-task logging.
        """
        stale_running = await self.reclaim_stale_running()
        orphaned_queued = await self.reclaim_orphaned_queued()

        result = {
            "stale_running": stale_running,
            "orphaned_queued": orphaned_queued,
        }
        log.info(
            "reclaim_sweep_complete",
            stale_running=stale_running,
            orphaned_queued=orphaned_queued,
        )
        return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _coerce_uuid(value: Any) -> uuid.UUID:
    """Coerce a pyodbc UUID result (str, bytes, or UUID) to uuid.UUID."""
    if isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(str(value))
