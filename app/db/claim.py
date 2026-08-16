"""
Atomic claim operations for the ``requests`` table.

Two functions that must be single-statement or explicit-lock operations
to avoid TOCTOU races under concurrent load:

1. ``atomic_insert_and_claim``
   Attempts an INSERT.  On duplicate-key violation of
   ``UX_requests_active_wip``, returns the existing row so the caller
   can apply the D-8 / D-9 idempotency decision (same key → 200,
   different key → 409).

2. ``atomic_claim_queued``
   Transitions a specific row from QUEUED → RUNNING in a single UPDATE
   that is conditioned on the row still being QUEUED.  Returns True if
   this worker won the race, False if another worker already owns it.

Neither function commits — the caller controls the transaction.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

import pyodbc  # type: ignore[import-untyped]

from app.domain.models import Request
from app.domain.states import Status


# ---------------------------------------------------------------------------
# Internal helpers (same as in repositories.py; duplicated to avoid circular
# imports — these are tiny and stable).
# ---------------------------------------------------------------------------


def _uuid(value: Any) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(str(value))


def _dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def _row_to_request(cursor: pyodbc.Cursor, row: Any) -> Request:
    cols = [d[0] for d in cursor.description]
    data = dict(zip(cols, row))
    return Request(
        request_id=_uuid(data["request_id"]),
        idempotency_key=data["idempotency_key"],
        action=data["action"],
        wip_fqdn=data["wip_fqdn"],
        target_device=data["target_device"],
        payload_hash=data["payload_hash"],
        payload_json=data["payload_json"],
        status=Status(data["status"]),
        created_at=_dt(data["created_at"]) or datetime.utcnow(),
        updated_at=_dt(data["updated_at"]) or datetime.utcnow(),
        started_at=_dt(data.get("started_at")),
        completed_at=_dt(data.get("completed_at")),
        worker_id=data.get("worker_id"),
        pod_id=data.get("pod_id"),
        last_heartbeat_at=_dt(data.get("last_heartbeat_at")),
        attempt_count=int(data.get("attempt_count", 0)),
        last_error=data.get("last_error"),
        needs_attention_reason=data.get("needs_attention_reason"),
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def atomic_insert_and_claim(
    conn: pyodbc.Connection,
    request_dict: dict[str, Any],
) -> tuple[bool, Request]:
    """
    Try to INSERT a new request row guarded by ``UX_requests_active_wip``.

    Parameters
    ----------
    conn:
        An open ``pyodbc.Connection``.  The caller is responsible for
        ``COMMIT`` or ``ROLLBACK``.
    request_dict:
        Column-value mapping for the new row.  Required keys:
        ``request_id``, ``idempotency_key``, ``action``, ``wip_fqdn``,
        ``target_device``, ``payload_hash``, ``payload_json``,
        ``status``.  All others are optional and default to NULL / DB
        defaults.

    Returns
    -------
    (True, new_request)
        INSERT succeeded — this is a genuinely new workflow.
    (False, existing_request)
        Duplicate-key on ``UX_requests_active_wip`` — an active workflow
        already exists for this FQDN.  The caller inspects
        ``existing_request.idempotency_key`` to decide between 200 (D-9)
        and 409 (D-8).

    Notes
    -----
    The duplicate-key error code for MSSQL is 2601 (unique index
    violation) or 2627 (primary-key / unique constraint).  Both are
    caught; only the FQDN guard index matters here, but being
    defensive costs nothing.

    The SELECT after a collision uses ``NOLOCK`` because the winning
    INSERT is uncommitted — we want the tentative row, not a read that
    blocks waiting for the winner to commit.  The caller must treat the
    returned row as potentially uncommitted if they rely on consistency;
    in practice, the API reads it to format a 409/200 response and does
    not take further write action based on it.
    """
    insert_sql = """
        INSERT INTO requests (
            request_id, idempotency_key, action, wip_fqdn, target_device,
            payload_hash, payload_json, status,
            created_at, updated_at, started_at, completed_at,
            worker_id, pod_id, last_heartbeat_at,
            attempt_count, last_error, needs_attention_reason
        ) VALUES (
            ?, ?, ?, ?, ?,
            ?, ?, ?,
            SYSUTCDATETIME(), SYSUTCDATETIME(), NULL, NULL,
            NULL, NULL, NULL,
            0, NULL, NULL
        )
    """
    insert_params = (
        str(request_dict["request_id"]),
        request_dict["idempotency_key"],
        request_dict["action"],
        request_dict["wip_fqdn"],
        request_dict["target_device"],
        request_dict["payload_hash"],
        request_dict["payload_json"],
        str(request_dict.get("status", Status.RECEIVED)),
    )

    cursor = conn.cursor()
    try:
        cursor.execute(insert_sql, insert_params)
        # INSERT succeeded; read back the freshly created row so we have
        # the DB-generated timestamps.
        cursor.execute(
            "SELECT * FROM requests WHERE request_id = ?",
            (str(request_dict["request_id"]),),
        )
        row = cursor.fetchone()
        if row is None:
            # Should be unreachable: we just inserted it.
            raise RuntimeError(
                f"INSERT succeeded but SELECT found no row for "
                f"request_id={request_dict['request_id']!r}"
            )
        return True, _row_to_request(cursor, row)

    except pyodbc.IntegrityError as exc:
        # MSSQL error 2601 = unique index violation
        # MSSQL error 2627 = unique constraint violation
        # Both indicate a duplicate FQDN in the active partial index.
        sql_state = exc.args[0] if exc.args else ""
        native_error = exc.args[1] if len(exc.args) > 1 else ""
        _is_duplicate = (
            "2601" in str(native_error)
            or "2627" in str(native_error)
            or "23000" in str(sql_state)  # ODBC SQLSTATE for integrity errors
        )
        if not _is_duplicate:
            raise

        # Retrieve the existing active row.
        select_sql = """
            SELECT * FROM requests WITH (NOLOCK)
            WHERE wip_fqdn = ?
              AND status IN (
                  'RECEIVED', 'VALIDATING', 'QUEUED', 'RUNNING', 'VERIFYING'
              )
        """
        cursor.execute(select_sql, (request_dict["wip_fqdn"],))
        existing_row = cursor.fetchone()
        if existing_row is None:
            # Extremely unlikely: the race winner has already completed and
            # left the active window.  Propagate as a retriable error.
            raise RuntimeError(
                "Duplicate key on UX_requests_active_wip but no active row "
                f"found for fqdn={request_dict['wip_fqdn']!r}.  "
                "The winning request may have just completed — retry the admission check."
            ) from exc
        return False, _row_to_request(cursor, existing_row)


def atomic_claim_queued(
    conn: pyodbc.Connection,
    request_id: uuid.UUID,
    worker_id: str,
    pod_id: str,
) -> bool:
    """
    Transition a QUEUED request to RUNNING in a single conditional UPDATE.

    The WHERE clause on ``status = 'QUEUED'`` guarantees exactly one
    worker wins even if multiple workers dequeue the same ``request_id``
    simultaneously (e.g., after a requeue following a timeout).

    Parameters
    ----------
    conn:
        Open ``pyodbc.Connection``.
    request_id:
        The request to claim.
    worker_id:
        Celery worker hostname or equivalent identifier.
    pod_id:
        OpenShift pod name — used by the reclaim sweeper to correlate
        stale locks with dead pods.

    Returns
    -------
    True
        This worker successfully claimed the row (1 row updated).
    False
        Another worker already owns the row, or it is no longer QUEUED
        (0 rows updated).  Caller should abort silently.
    """
    sql = """
        UPDATE requests
        SET
            status            = 'RUNNING',
            worker_id         = ?,
            pod_id            = ?,
            started_at        = SYSUTCDATETIME(),
            last_heartbeat_at = SYSUTCDATETIME(),
            updated_at        = SYSUTCDATETIME()
        WHERE
            request_id = ?
            AND status = 'QUEUED'
    """
    params = (worker_id, pod_id, str(request_id))
    cursor = conn.cursor()
    cursor.execute(sql, params)
    return cursor.rowcount == 1
