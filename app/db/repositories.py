"""
Repository layer — raw pyodbc, no ORM.

Each repository receives a live ``pyodbc.Connection`` in its constructor.
Connection lifecycle (open, commit, rollback, close) is the caller's
responsibility.

Rules enforced throughout:
- Every query uses parameterised placeholders (?). String-format SQL is
  never used.
- Repositories do not call conn.commit(); the caller controls transactions.
- ``SELECT`` results are mapped back to domain objects via ``_row_to_*``
  helpers so the SQL-to-domain mapping lives in one place.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Optional

import pyodbc  # type: ignore[import-untyped]

from app.domain.models import (
    ManagedObject,
    RemediationItem,
    Request,
    RequestStep,
    StateTransition,
)
from app.domain.states import Status


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _uuid(value: Any) -> uuid.UUID:
    """Coerce a pyodbc UUID result (str or bytes) to a proper UUID."""
    if isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(str(value))


def _dt(value: Any) -> Optional[datetime]:
    """Return a datetime or None; pass through existing datetime objects."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


# ---------------------------------------------------------------------------
# RequestRepository
# ---------------------------------------------------------------------------


class RequestRepository:
    """CRUD + status helpers for the ``requests`` table."""

    def __init__(self, conn: pyodbc.Connection) -> None:
        self._conn = conn

    # ------------------------------------------------------------------
    def insert(self, request: Request) -> Request:
        """
        Insert a new row and return the persisted domain object.

        Does *not* handle duplicate-key errors — use
        ``atomic_insert_and_claim`` in ``app.db.claim`` for that path.
        """
        sql = """
            INSERT INTO requests (
                request_id, idempotency_key, action, wip_fqdn, target_device,
                payload_hash, payload_json, status,
                created_at, updated_at, started_at, completed_at,
                worker_id, pod_id, last_heartbeat_at,
                attempt_count, last_error, needs_attention_reason
            ) VALUES (
                ?, ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?
            )
        """
        params = (
            str(request.request_id),
            request.idempotency_key,
            request.action,
            request.wip_fqdn,
            request.target_device,
            request.payload_hash,
            request.payload_json,
            str(request.status),
            request.created_at,
            request.updated_at,
            request.started_at,
            request.completed_at,
            request.worker_id,
            request.pod_id,
            request.last_heartbeat_at,
            request.attempt_count,
            request.last_error,
            request.needs_attention_reason,
        )
        cursor = self._conn.cursor()
        cursor.execute(sql, params)
        return request

    # ------------------------------------------------------------------
    def get_by_id(self, request_id: uuid.UUID) -> Optional[Request]:
        """Return a Request by its primary key, or None if not found."""
        sql = "SELECT * FROM requests WHERE request_id = ?"
        cursor = self._conn.cursor()
        cursor.execute(sql, (str(request_id),))
        row = cursor.fetchone()
        if row is None:
            return None
        return self._row_to_request(row, cursor)

    # ------------------------------------------------------------------
    def get_by_fqdn_active(self, fqdn: str) -> Optional[Request]:
        """
        Return the active (non-terminal) request for a given WideIP FQDN,
        or None.

        This mirrors the partial index ``UX_requests_active_wip`` and is
        used by the idempotency path to retrieve the existing row when an
        INSERT would violate the unique constraint.
        """
        sql = """
            SELECT * FROM requests
            WHERE wip_fqdn = ?
              AND status IN (
                  'RECEIVED', 'VALIDATING', 'QUEUED', 'RUNNING', 'VERIFYING'
              )
        """
        cursor = self._conn.cursor()
        cursor.execute(sql, (fqdn,))
        row = cursor.fetchone()
        if row is None:
            return None
        return self._row_to_request(row, cursor)

    # ------------------------------------------------------------------
    def update_status(
        self,
        request_id: uuid.UUID,
        status: Status,
        **fields: Any,
    ) -> None:
        """
        Update ``status`` and any additional keyword-argument columns.

        Only a fixed allowlist of columns may be updated this way to
        prevent accidental SQL injection through field names.

        Allowed extra fields:
            started_at, completed_at, worker_id, pod_id,
            last_heartbeat_at, attempt_count, last_error,
            needs_attention_reason
        """
        _ALLOWED = frozenset(
            {
                "started_at",
                "completed_at",
                "worker_id",
                "pod_id",
                "last_heartbeat_at",
                "attempt_count",
                "last_error",
                "needs_attention_reason",
            }
        )
        unknown = set(fields) - _ALLOWED
        if unknown:
            raise ValueError(f"update_status: unknown field(s): {unknown!r}")

        set_clauses = ["status = ?", "updated_at = SYSUTCDATETIME()"]
        params: list[Any] = [str(status)]

        for col in sorted(fields):  # sorted for determinism in tests
            set_clauses.append(f"{col} = ?")
            params.append(fields[col])

        params.append(str(request_id))

        sql = f"UPDATE requests SET {', '.join(set_clauses)} WHERE request_id = ?"
        cursor = self._conn.cursor()
        cursor.execute(sql, params)

    # ------------------------------------------------------------------
    def update_heartbeat(self, request_id: uuid.UUID) -> None:
        """Stamp ``last_heartbeat_at`` with the current UTC time."""
        sql = """
            UPDATE requests
            SET last_heartbeat_at = SYSUTCDATETIME(),
                updated_at        = SYSUTCDATETIME()
            WHERE request_id = ?
        """
        cursor = self._conn.cursor()
        cursor.execute(sql, (str(request_id),))

    # ------------------------------------------------------------------
    @staticmethod
    def _row_to_request(row: Any, cursor: pyodbc.Cursor) -> Request:
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
# RequestStepRepository
# ---------------------------------------------------------------------------


class RequestStepRepository:
    """CRUD for the ``request_steps`` table."""

    def __init__(self, conn: pyodbc.Connection) -> None:
        self._conn = conn

    # ------------------------------------------------------------------
    def insert(self, step: RequestStep) -> RequestStep:
        sql = """
            INSERT INTO request_steps (
                step_id, request_id, step_name, step_order,
                target_system, object_type, object_key,
                intent_json, pre_state_json, result_json,
                status, attempts, error,
                started_at, completed_at, compensation_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            str(step.step_id),
            str(step.request_id),
            step.step_name,
            step.step_order,
            step.target_system,
            step.object_type,
            step.object_key,
            step.intent_json,
            step.pre_state_json,
            step.result_json,
            step.status,
            step.attempts,
            step.error,
            step.started_at,
            step.completed_at,
            step.compensation_status,
        )
        cursor = self._conn.cursor()
        cursor.execute(sql, params)
        return step

    # ------------------------------------------------------------------
    def update(self, step_id: uuid.UUID, **fields: Any) -> None:
        """
        Update one or more columns on a step row.

        Allowed fields:
            pre_state_json, result_json, status, attempts, error,
            started_at, completed_at, compensation_status
        """
        _ALLOWED = frozenset(
            {
                "pre_state_json",
                "result_json",
                "status",
                "attempts",
                "error",
                "started_at",
                "completed_at",
                "compensation_status",
            }
        )
        unknown = set(fields) - _ALLOWED
        if unknown:
            raise ValueError(f"update step: unknown field(s): {unknown!r}")

        if not fields:
            return

        set_clauses = [f"{col} = ?" for col in sorted(fields)]
        params: list[Any] = [fields[col] for col in sorted(fields)]
        params.append(str(step_id))

        sql = f"UPDATE request_steps SET {', '.join(set_clauses)} WHERE step_id = ?"
        cursor = self._conn.cursor()
        cursor.execute(sql, params)

    # ------------------------------------------------------------------
    def get_steps(self, request_id: uuid.UUID) -> list[RequestStep]:
        """Return all steps for a request, ordered by step_order ascending."""
        sql = """
            SELECT * FROM request_steps
            WHERE request_id = ?
            ORDER BY step_order ASC
        """
        cursor = self._conn.cursor()
        cursor.execute(sql, (str(request_id),))
        rows = cursor.fetchall()
        return [self._row_to_step(r, cursor) for r in rows]

    # ------------------------------------------------------------------
    def get_step(self, step_id: uuid.UUID) -> Optional[RequestStep]:
        sql = "SELECT * FROM request_steps WHERE step_id = ?"
        cursor = self._conn.cursor()
        cursor.execute(sql, (str(step_id),))
        row = cursor.fetchone()
        if row is None:
            return None
        return self._row_to_step(row, cursor)

    # ------------------------------------------------------------------
    @staticmethod
    def _row_to_step(row: Any, cursor: pyodbc.Cursor) -> RequestStep:
        cols = [d[0] for d in cursor.description]
        data = dict(zip(cols, row))
        return RequestStep(
            step_id=_uuid(data["step_id"]),
            request_id=_uuid(data["request_id"]),
            step_name=data["step_name"],
            step_order=int(data["step_order"]),
            target_system=data["target_system"],
            object_type=data["object_type"],
            object_key=data["object_key"],
            intent_json=data["intent_json"],
            pre_state_json=data.get("pre_state_json"),
            result_json=data.get("result_json"),
            status=data["status"],
            attempts=int(data.get("attempts", 0)),
            error=data.get("error"),
            started_at=_dt(data.get("started_at")),
            completed_at=_dt(data.get("completed_at")),
            compensation_status=data.get("compensation_status"),
        )


# ---------------------------------------------------------------------------
# ManagedObjectRepository
# ---------------------------------------------------------------------------


class ManagedObjectRepository:
    """CRUD for the ``managed_objects`` table."""

    def __init__(self, conn: pyodbc.Connection) -> None:
        self._conn = conn

    # ------------------------------------------------------------------
    def upsert(self, obj: ManagedObject) -> ManagedObject:
        """
        INSERT a new managed object or UPDATE the existing row that
        matches on (object_type, object_key, target_device).

        MSSQL MERGE is used to make this a single round-trip.
        """
        sql = """
            MERGE managed_objects WITH (HOLDLOCK) AS target
            USING (
                SELECT
                    ? AS object_id,
                    ? AS wip_fqdn,
                    ? AS object_type,
                    ? AS object_key,
                    ? AS target_device,
                    ? AS desired_state_json,
                    ? AS last_verified_at,
                    ? AS drift_detected_at,
                    ? AS drift_details_json,
                    ? AS owning_request_id,
                    ? AS status
            ) AS src
            ON  target.object_type   = src.object_type
            AND target.object_key    = src.object_key
            AND target.target_device = src.target_device
            WHEN MATCHED THEN
                UPDATE SET
                    wip_fqdn           = src.wip_fqdn,
                    desired_state_json = src.desired_state_json,
                    last_verified_at   = src.last_verified_at,
                    drift_detected_at  = src.drift_detected_at,
                    drift_details_json = src.drift_details_json,
                    owning_request_id  = src.owning_request_id,
                    status             = src.status
            WHEN NOT MATCHED THEN
                INSERT (
                    object_id, wip_fqdn, object_type, object_key, target_device,
                    desired_state_json, last_verified_at, drift_detected_at,
                    drift_details_json, owning_request_id, status
                )
                VALUES (
                    src.object_id, src.wip_fqdn, src.object_type, src.object_key,
                    src.target_device, src.desired_state_json, src.last_verified_at,
                    src.drift_detected_at, src.drift_details_json,
                    src.owning_request_id, src.status
                );
        """
        params = (
            str(obj.object_id),
            obj.wip_fqdn,
            obj.object_type,
            obj.object_key,
            obj.target_device,
            obj.desired_state_json,
            obj.last_verified_at,
            obj.drift_detected_at,
            obj.drift_details_json,
            str(obj.owning_request_id) if obj.owning_request_id else None,
            obj.status,
        )
        cursor = self._conn.cursor()
        cursor.execute(sql, params)
        return obj

    # ------------------------------------------------------------------
    def get_by_fqdn(self, fqdn: str) -> list[ManagedObject]:
        sql = "SELECT * FROM managed_objects WHERE wip_fqdn = ?"
        cursor = self._conn.cursor()
        cursor.execute(sql, (fqdn,))
        rows = cursor.fetchall()
        return [self._row_to_obj(r, cursor) for r in rows]

    # ------------------------------------------------------------------
    @staticmethod
    def _row_to_obj(row: Any, cursor: pyodbc.Cursor) -> ManagedObject:
        cols = [d[0] for d in cursor.description]
        data = dict(zip(cols, row))
        return ManagedObject(
            object_id=_uuid(data["object_id"]),
            wip_fqdn=data["wip_fqdn"],
            object_type=data["object_type"],
            object_key=data["object_key"],
            target_device=data["target_device"],
            desired_state_json=data["desired_state_json"],
            last_verified_at=_dt(data.get("last_verified_at")),
            drift_detected_at=_dt(data.get("drift_detected_at")),
            drift_details_json=data.get("drift_details_json"),
            owning_request_id=_uuid(data["owning_request_id"])
            if data.get("owning_request_id")
            else None,
            status=data["status"],
        )


# ---------------------------------------------------------------------------
# StateTransitionRepository
# ---------------------------------------------------------------------------


class StateTransitionRepository:
    """Append-only writer for the ``state_transitions`` audit table."""

    def __init__(self, conn: pyodbc.Connection) -> None:
        self._conn = conn

    # ------------------------------------------------------------------
    def record(
        self,
        request_id: uuid.UUID,
        from_status: Status,
        to_status: Status,
        reason: str,
        actor: str,
    ) -> None:
        """Append one transition record. Never updates or deletes."""
        sql = """
            INSERT INTO state_transitions
                (request_id, from_status, to_status, reason, actor, timestamp)
            VALUES (?, ?, ?, ?, ?, SYSUTCDATETIME())
        """
        params = (
            str(request_id),
            str(from_status),
            str(to_status),
            reason,
            actor,
        )
        cursor = self._conn.cursor()
        cursor.execute(sql, params)


# ---------------------------------------------------------------------------
# RemediationRepository
# ---------------------------------------------------------------------------


class RemediationRepository:
    """Queue management for the ``remediation_queue`` table."""

    def __init__(self, conn: pyodbc.Connection) -> None:
        self._conn = conn

    # ------------------------------------------------------------------
    def enqueue(self, item: RemediationItem) -> RemediationItem:
        """Insert a new remediation item."""
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
        cursor = self._conn.cursor()
        cursor.execute(sql, params)
        return item

    # ------------------------------------------------------------------
    def get_due(self, limit: int = 50) -> list[RemediationItem]:
        """
        Return up to *limit* items whose ``next_retry_at`` is in the past
        (or NULL), are not escalated, and have no resolution yet.

        Results are ordered by ``next_retry_at`` ascending so the oldest
        due items are processed first.
        """
        sql = """
            SELECT TOP (?) *
            FROM remediation_queue
            WHERE escalated_at IS NULL
              AND resolution  IS NULL
              AND (next_retry_at IS NULL OR next_retry_at <= SYSUTCDATETIME())
            ORDER BY next_retry_at ASC
        """
        cursor = self._conn.cursor()
        cursor.execute(sql, (limit,))
        rows = cursor.fetchall()
        return [self._row_to_item(r, cursor) for r in rows]

    # ------------------------------------------------------------------
    def update(self, remediation_id: uuid.UUID, **fields: Any) -> None:
        """
        Update one or more columns on a remediation row.

        Allowed fields:
            retry_count, next_retry_at, escalated_at, resolution
        """
        _ALLOWED = frozenset(
            {"retry_count", "next_retry_at", "escalated_at", "resolution"}
        )
        unknown = set(fields) - _ALLOWED
        if unknown:
            raise ValueError(f"update remediation: unknown field(s): {unknown!r}")

        if not fields:
            return

        set_clauses = [f"{col} = ?" for col in sorted(fields)]
        params: list[Any] = [fields[col] for col in sorted(fields)]
        params.append(str(remediation_id))

        sql = (
            f"UPDATE remediation_queue SET {', '.join(set_clauses)} "
            f"WHERE remediation_id = ?"
        )
        cursor = self._conn.cursor()
        cursor.execute(sql, params)

    # ------------------------------------------------------------------
    @staticmethod
    def _row_to_item(row: Any, cursor: pyodbc.Cursor) -> RemediationItem:
        cols = [d[0] for d in cursor.description]
        data = dict(zip(cols, row))
        return RemediationItem(
            remediation_id=_uuid(data["remediation_id"]),
            request_id=_uuid(data["request_id"]),
            step_id=_uuid(data["step_id"]),
            failure_category=data["failure_category"],
            retry_count=int(data.get("retry_count", 0)),
            next_retry_at=_dt(data.get("next_retry_at")),
            escalated_at=_dt(data.get("escalated_at")),
            resolution=data.get("resolution"),
        )
