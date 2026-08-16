"""
app/recovery/remediation.py — Failed step retry queue (T-5.2).

Handles the "WideIP created, CNAME failed" class of failure and similar
partial-success scenarios where the desired end-state is still achievable
with retries.

Failure categories (§7 of the plan)
------------------------------------
  monitor_create_failed           — monitor step failed; nothing written to F5 yet
  pool_create_failed              — pool step failed; monitor may have been created
  wideip_create_failed            — WideIP step failed; monitor+pool may exist
  cname_create_failed             — CNAME step failed; F5 objects are all present
  cname_failed_after_wideip       — WideIP succeeded but CNAME retry cap hit
  post_validation_mismatch        — read-back differs from intent after write
  rollback_failed                 — compensating step itself failed

Retry schedule
--------------
Exponential backoff with jitter.  The schedule is:

    attempt 1 → BASE_BACKOFF_SECONDS × 2^1 = 60 s   (+ 0–10 s jitter)
    attempt 2 → 120 s + jitter
    attempt 3 → 240 s + jitter
    attempt 4 → 480 s + jitter
    attempt 5 → capped at MAX_BACKOFF_SECONDS (3 600 s) + jitter

After MAX_ATTEMPTS the item is escalated to NEEDS_ATTENTION and a
notification is sent.  NEEDS_ATTENTION is terminal — nothing automatic
exits it.

P-n parameters
--------------
None of the timing constants here are P-n parameters (they are policy
decisions driven by the failure scenarios rather than measured load).
They are class-level constants so they can be overridden in subclasses or
patched in tests without constructor complexity.
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from app.core.logging import get_logger
from app.domain.models import RemediationItem

log = get_logger(__name__)

# Failure categories defined in §7 of the plan.
FAILURE_CATEGORIES: frozenset[str] = frozenset(
    {
        "monitor_create_failed",
        "pool_create_failed",
        "wideip_create_failed",
        "cname_create_failed",
        "cname_failed_after_wideip",
        "post_validation_mismatch",
        "rollback_failed",
    }
)


class RemediationWorker:
    """
    Process the ``remediation_queue`` table: retry failed steps with
    exponential backoff, escalate to NEEDS_ATTENTION after ``MAX_ATTEMPTS``.

    Parameters
    ----------
    db_conn_factory:
        Zero-argument callable that returns an open ``pyodbc.Connection``.
    celery_app:
        Celery application instance used to re-enqueue workflow tasks.
    notification_sender:
        Callable that accepts ``(request_id: str, failure_category: str,
        diagnostic: dict)`` and sends an alert (e.g. to PagerDuty, Slack).
        Called on every NEEDS_ATTENTION escalation.
    workflow_task_name:
        Celery task name for re-enqueueing.
    """

    MAX_ATTEMPTS: int = 5
    BASE_BACKOFF_SECONDS: int = 30
    MAX_BACKOFF_SECONDS: int = 3600
    _JITTER_MAX_SECONDS: int = 10

    def __init__(
        self,
        db_conn_factory: Callable[[], Any],
        celery_app: Any,
        notification_sender: Callable[..., Any],
        workflow_task_name: str = "tasks.workflows.run_workflow",
    ) -> None:
        self._db_conn_factory = db_conn_factory
        self._celery_app = celery_app
        self._notify = notification_sender
        self._workflow_task_name = workflow_task_name

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def process_due_items(self, limit: int = 50) -> int:
        """
        Query for remediation items due for retry and process each one.

        For each item:
          1. Increment ``retry_count``.
          2. If ``retry_count > MAX_ATTEMPTS`` → escalate to NEEDS_ATTENTION:
               - Update ``requests.status`` to NEEDS_ATTENTION.
               - Set ``escalated_at`` on the remediation row.
               - Send a notification.
          3. Otherwise → re-enqueue the workflow task and compute
             ``next_retry_at`` with exponential backoff + jitter.

        Parameters
        ----------
        limit:
            Maximum items to process per call.  Guards against processing
            a flooded queue in one shot.

        Returns
        -------
        int
            Number of items processed (escalated + re-enqueued combined).
        """
        conn = self._db_conn_factory()
        processed = 0
        try:
            # Fetch due items.
            items = _get_due_items(conn, limit)

            for item in items:
                new_retry_count = item.retry_count + 1

                if new_retry_count > self.MAX_ATTEMPTS:
                    await self._escalate(conn, item, new_retry_count)
                else:
                    await self._retry(conn, item, new_retry_count)

                conn.commit()
                processed += 1

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

        return processed

    async def enqueue_for_remediation(
        self,
        request_id: uuid.UUID,
        step_id: uuid.UUID,
        failure_category: str,
    ) -> None:
        """
        Insert a new entry into ``remediation_queue`` with
        ``next_retry_at = now + BASE_BACKOFF_SECONDS``.

        Parameters
        ----------
        request_id:
            The provisioning request that failed.
        step_id:
            The specific step that failed (from ``request_steps``).
        failure_category:
            One of the categories in FAILURE_CATEGORIES.  The value is
            stored for diagnostic purposes and drives notification content.
        """
        if failure_category not in FAILURE_CATEGORIES:
            log.warning(
                "enqueue_for_remediation_unknown_category",
                failure_category=failure_category,
                request_id=str(request_id),
            )

        import datetime as dt

        now = _utcnow()
        # First retry after BASE_BACKOFF_SECONDS.
        next_retry_at = now + dt.timedelta(seconds=self.BASE_BACKOFF_SECONDS)

        item = RemediationItem(
            remediation_id=uuid.uuid4(),
            request_id=request_id,
            step_id=step_id,
            failure_category=failure_category,
            retry_count=0,
            next_retry_at=next_retry_at,
        )

        conn = self._db_conn_factory()
        try:
            _insert_remediation_item(conn, item)
            conn.commit()
            log.info(
                "enqueued_for_remediation",
                request_id=str(request_id),
                step_id=str(step_id),
                failure_category=failure_category,
                next_retry_at=next_retry_at.isoformat(),
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

    # ------------------------------------------------------------------
    # Backoff computation
    # ------------------------------------------------------------------

    def _compute_next_retry(self, retry_count: int) -> datetime:
        """
        Compute the datetime of the next retry attempt.

        Schedule (before jitter):
            attempt 1 → BASE_BACKOFF × 2^1 =   60 s
            attempt 2 → BASE_BACKOFF × 2^2 =  120 s
            attempt 3 → BASE_BACKOFF × 2^3 =  240 s
            attempt 4 → BASE_BACKOFF × 2^4 =  480 s
            attempt 5 → BASE_BACKOFF × 2^5 =  960 s  (well below MAX_BACKOFF)

        All values are capped at MAX_BACKOFF_SECONDS before adding jitter.
        Jitter is uniformly distributed in [0, _JITTER_MAX_SECONDS].

        Parameters
        ----------
        retry_count:
            The retry_count value AFTER incrementing (i.e. the count that
            will be stored on this attempt).
        """
        import datetime as dt

        base = self.BASE_BACKOFF_SECONDS * (2 ** retry_count)
        capped = min(base, self.MAX_BACKOFF_SECONDS)
        jitter = random.uniform(0, self._JITTER_MAX_SECONDS)  # noqa: S311
        delay_seconds = capped + jitter
        return _utcnow() + dt.timedelta(seconds=delay_seconds)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _escalate(
        self,
        conn: Any,
        item: RemediationItem,
        new_retry_count: int,
    ) -> None:
        """Escalate to NEEDS_ATTENTION, notify, and mark escalated_at."""
        now = _utcnow()
        log.error(
            "remediation_escalated_to_needs_attention",
            request_id=str(item.request_id),
            step_id=str(item.step_id),
            failure_category=item.failure_category,
            retry_count=new_retry_count,
        )

        # Mark request as NEEDS_ATTENTION.
        _update_request_needs_attention(
            conn,
            item.request_id,
            reason=(
                f"Remediation cap reached after {new_retry_count} attempts. "
                f"failure_category={item.failure_category}"
            ),
        )

        # Mark the remediation row as escalated.
        _update_remediation_row(
            conn,
            item.remediation_id,
            retry_count=new_retry_count,
            escalated_at=now,
        )

        # Send notification.  Failures here must not suppress the DB write.
        try:
            await _call_maybe_async(
                self._notify,
                str(item.request_id),
                item.failure_category,
                {
                    "step_id": str(item.step_id),
                    "retry_count": new_retry_count,
                    "remediation_id": str(item.remediation_id),
                },
            )
        except Exception as exc:
            log.error(
                "remediation_notification_failed",
                request_id=str(item.request_id),
                error=str(exc),
            )

    async def _retry(
        self,
        conn: Any,
        item: RemediationItem,
        new_retry_count: int,
    ) -> None:
        """Increment retry_count, compute next_retry_at, and re-enqueue."""
        next_retry_at = self._compute_next_retry(new_retry_count)

        _update_remediation_row(
            conn,
            item.remediation_id,
            retry_count=new_retry_count,
            next_retry_at=next_retry_at,
        )

        # Re-enqueue request_id ONLY (D-2).
        self._celery_app.send_task(
            self._workflow_task_name,
            args=[str(item.request_id)],
        )

        log.info(
            "remediation_requeued",
            request_id=str(item.request_id),
            step_id=str(item.step_id),
            failure_category=item.failure_category,
            retry_count=new_retry_count,
            next_retry_at=next_retry_at.isoformat(),
        )


# ---------------------------------------------------------------------------
# Database helpers (thin wrappers; keep SQL here not in the class)
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    """Return current UTC datetime, timezone-aware."""
    return datetime.now(tz=timezone.utc)


def _get_due_items(conn: Any, limit: int) -> list[RemediationItem]:
    """
    Fetch remediation rows due for processing.

    Mirrors RemediationRepository.get_due() but does not depend on the
    repository class to keep the recovery module self-contained.
    """
    sql = """
        SELECT TOP (?)
            remediation_id, request_id, step_id,
            failure_category, retry_count, next_retry_at,
            escalated_at, resolution
        FROM remediation_queue
        WHERE escalated_at IS NULL
          AND resolution   IS NULL
          AND (next_retry_at IS NULL OR next_retry_at <= GETUTCDATE())
        ORDER BY next_retry_at ASC
    """
    cursor = conn.cursor()
    cursor.execute(sql, (limit,))
    rows = cursor.fetchall()
    items = []
    for row in rows:
        cols = [d[0] for d in cursor.description]
        data = dict(zip(cols, row))
        items.append(
            RemediationItem(
                remediation_id=_coerce_uuid(data["remediation_id"]),
                request_id=_coerce_uuid(data["request_id"]),
                step_id=_coerce_uuid(data["step_id"]),
                failure_category=data["failure_category"],
                retry_count=int(data.get("retry_count", 0)),
                next_retry_at=_dt(data.get("next_retry_at")),
                escalated_at=_dt(data.get("escalated_at")),
                resolution=data.get("resolution"),
            )
        )
    return items


def _insert_remediation_item(conn: Any, item: RemediationItem) -> None:
    sql = """
        INSERT INTO remediation_queue (
            remediation_id, request_id, step_id,
            failure_category, retry_count, next_retry_at,
            escalated_at, resolution
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = (
        str(item.remediation_id),
        str(item.request_id),
        str(item.step_id),
        item.failure_category,
        item.retry_count,
        item.next_retry_at,
        item.escalated_at,
        item.resolution,
    )
    conn.cursor().execute(sql, params)


def _update_remediation_row(
    conn: Any,
    remediation_id: uuid.UUID,
    **fields: Any,
) -> None:
    """Update allowed columns on a remediation_queue row."""
    _ALLOWED = frozenset({"retry_count", "next_retry_at", "escalated_at", "resolution"})
    unknown = set(fields) - _ALLOWED
    if unknown:
        raise ValueError(f"_update_remediation_row: unknown field(s): {unknown!r}")

    if not fields:
        return

    set_clauses = [f"{col} = ?" for col in sorted(fields)]
    params: list[Any] = [fields[col] for col in sorted(fields)]
    params.append(str(remediation_id))

    sql = (
        f"UPDATE remediation_queue SET {', '.join(set_clauses)} "
        f"WHERE remediation_id = ?"
    )
    conn.cursor().execute(sql, params)


def _update_request_needs_attention(
    conn: Any,
    request_id: uuid.UUID,
    reason: str,
) -> None:
    """Transition a request to NEEDS_ATTENTION with a reason."""
    sql = """
        UPDATE requests
        SET status                  = 'NEEDS_ATTENTION',
            needs_attention_reason  = ?,
            updated_at              = GETUTCDATE()
        WHERE request_id = ?
    """
    conn.cursor().execute(sql, (reason, str(request_id)))


async def _call_maybe_async(fn: Any, *args: Any, **kwargs: Any) -> Any:
    """Call a function that may be sync or async."""
    import asyncio
    import inspect

    result = fn(*args, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


def _coerce_uuid(value: Any) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(str(value))


def _dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))
