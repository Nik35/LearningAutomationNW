"""
Notification polling endpoint — GET /api/v1/notifications

Called every 1 minute by consuming applications to check for alerts.
Returns NEEDS_ATTENTION requests, recently opened circuit breakers,
and any ROLLBACK_FAILED entries since the last poll.

Design: pull-based (caller polls), not push. No webhook or SSE.
The `since` query parameter allows incremental polls (only items newer than
the caller's last check timestamp).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated

import pyodbc
import structlog
from fastapi import APIRouter, Depends, Query, Request

from app.core.config import settings

log = structlog.get_logger(__name__)

router = APIRouter()


# ── Response shape ─────────────────────────────────────────────────────────────

def _row_to_dict(row) -> dict:
    """Convert a pyodbc Row to a plain dict using column names."""
    columns = [col[0] for col in row.cursor_description]
    return dict(zip(columns, row))


# ── GET /notifications ─────────────────────────────────────────────────────────

@router.get("/notifications")
async def get_notifications(
    request: Request,
    since: Annotated[
        datetime | None,
        Query(description="Return only items updated after this ISO-8601 timestamp. "
                          "Omit for first call; use the `polled_at` from the previous response.")
    ] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> dict:
    """
    Returns all actionable alerts for the consuming application.

    Designed to be called on a 1-minute polling interval.

    Response shape:
    {
        "polled_at": "<ISO timestamp — use as `since` on next call>",
        "needs_attention": [
            {
                "request_id": "...",
                "wip_fqdn": "...",
                "target_device": "...",
                "action": "...",
                "needs_attention_reason": "...",
                "updated_at": "..."
            }
        ],
        "rollback_failed": [ ... same shape ... ],
        "open_breakers": [
            {
                "device_id": "...",
                "state": "open",
                "checked_at": "..."
            }
        ],
        "remediation_escalated": [
            {
                "request_id": "...",
                "step_id": "...",
                "failure_category": "...",
                "retry_count": 5,
                "escalated_at": "..."
            }
        ],
        "summary": {
            "needs_attention_count": 0,
            "rollback_failed_count": 0,
            "open_breaker_count": 0,
            "escalated_remediation_count": 0,
            "total_alerts": 0
        }
    }
    """
    polled_at = datetime.now(timezone.utc)
    since_clause = ""
    since_param = []
    if since is not None:
        since_clause = "AND updated_at > ?"
        since_param = [since]

    conn_str = settings.DB_CONNECTION_STRING
    redis_client = request.app.state.redis

    needs_attention: list[dict] = []
    rollback_failed: list[dict] = []
    remediation_escalated: list[dict] = []

    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # ── NEEDS_ATTENTION requests ───────────────────────────────────────
        cursor.execute(
            f"""
            SELECT TOP (?)
                request_id, wip_fqdn, target_device, action,
                needs_attention_reason, updated_at, last_error
            FROM requests
            WHERE status = 'NEEDS_ATTENTION'
            {since_clause}
            ORDER BY updated_at DESC
            """,
            [limit] + since_param,
        )
        for row in cursor.fetchall():
            d = _row_to_dict(row)
            d["request_id"] = str(d["request_id"])
            d["updated_at"] = d["updated_at"].isoformat() if d["updated_at"] else None
            needs_attention.append(d)

        # ── ROLLBACK_FAILED requests ───────────────────────────────────────
        cursor.execute(
            f"""
            SELECT TOP (?)
                request_id, wip_fqdn, target_device, action,
                needs_attention_reason, updated_at, last_error
            FROM requests
            WHERE status = 'ROLLBACK_FAILED'
            {since_clause}
            ORDER BY updated_at DESC
            """,
            [limit] + since_param,
        )
        for row in cursor.fetchall():
            d = _row_to_dict(row)
            d["request_id"] = str(d["request_id"])
            d["updated_at"] = d["updated_at"].isoformat() if d["updated_at"] else None
            rollback_failed.append(d)

        # ── Escalated remediation items ────────────────────────────────────
        escalated_since = "AND rq.escalated_at > ?" if since is not None else ""
        cursor.execute(
            f"""
            SELECT TOP (?)
                rq.remediation_id, rq.request_id, rq.step_id,
                rq.failure_category, rq.retry_count, rq.escalated_at
            FROM remediation_queue rq
            WHERE rq.escalated_at IS NOT NULL
              AND rq.resolution IS NULL
            {escalated_since}
            ORDER BY rq.escalated_at DESC
            """,
            [limit] + (since_param if since else []),
        )
        for row in cursor.fetchall():
            d = _row_to_dict(row)
            d["remediation_id"] = str(d["remediation_id"])
            d["request_id"] = str(d["request_id"])
            d["step_id"] = str(d["step_id"])
            d["escalated_at"] = d["escalated_at"].isoformat() if d["escalated_at"] else None
            remediation_escalated.append(d)

        cursor.close()
        conn.close()

    except Exception as exc:
        log.error("notifications.db_error", error=str(exc))
        # Return partial data rather than failing the poll entirely

    # ── Open circuit breakers ──────────────────────────────────────────────
    open_breakers: list[dict] = []
    try:
        for device_id in settings.KNOWN_DEVICE_IDS:
            state_key = f"breaker:{device_id}:state"
            state = await redis_client.get(state_key)
            if state == "open":
                open_breakers.append({
                    "device_id": device_id,
                    "state": "open",
                    "checked_at": polled_at.isoformat(),
                })
    except Exception as exc:
        log.error("notifications.redis_error", error=str(exc))

    total = (
        len(needs_attention)
        + len(rollback_failed)
        + len(open_breakers)
        + len(remediation_escalated)
    )

    return {
        "polled_at": polled_at.isoformat(),
        "needs_attention": needs_attention,
        "rollback_failed": rollback_failed,
        "open_breakers": open_breakers,
        "remediation_escalated": remediation_escalated,
        "summary": {
            "needs_attention_count": len(needs_attention),
            "rollback_failed_count": len(rollback_failed),
            "open_breaker_count": len(open_breakers),
            "escalated_remediation_count": len(remediation_escalated),
            "total_alerts": total,
        },
    }
