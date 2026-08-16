"""
app/recovery/reconciler.py — Drift detection sweep (WP-6, T-6.1 through T-6.3).

CRITICAL — D-10 is ABSOLUTE
-----------------------------
The reconciler is permanently report-only.  Passing ``write_enabled=True``
raises ``ValueError`` immediately.  There is no override path.

Rationale (from the plan, §1 D-10):
    "An auto-deleting reconciler on first prod run is the most destructive
     possible failure."  This estate carries inherited drift from a previous
     Ansible implementation.  The reconciler must produce reports first,
     earn trust, and only then consider convergence — per category,
     deliberately, with explicit opt-in.

Architecture
------------
- Reads from ``managed_objects`` in MSSQL (paginated, never enumerate all).
- Compares each record to the corresponding object in F5 and/or Infoblox
  by calling the injected client interfaces.
- Classifies any discrepancy with a ``DriftCategory``.
- Returns a list of ``DriftItem`` for structured logging and reporting.
- WRITES NOTHING to F5, Infoblox, or MSSQL managed_objects.

Client interfaces
-----------------
``f5_clients`` and ``infoblox_client`` are injected as opaque objects
(typed as ``Any``).  This module does not import from ``app.clients.*``
directly — the client layer is built by a separate agent and may not exist
yet.  Protocol stubs are defined below for documentation and type checking.

Page size
---------
``page_size`` limits how many DB rows are loaded at once.  A full sweep
of a large estate should run in multiple pages.  Progress is checkpointed
via the ``last_verified_at`` column (T-6.4: prioritise stalest objects).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Protocol, runtime_checkable

from app.core.logging import get_logger

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Drift classification
# ---------------------------------------------------------------------------


class DriftCategory(str, Enum):
    """Categories of detected drift, per §8 of the implementation plan."""

    IN_DB_MISSING_IN_F5 = "in_db_missing_in_f5"
    """Object in MSSQL but absent from F5.  Default: flag only."""

    IN_F5_NOT_IN_DB = "in_f5_not_in_db"
    """Object present in F5 but not tracked in MSSQL.  Never auto-delete."""

    ATTRIBUTES_DIFFER = "attributes_differ"
    """Both present but attributes do not match desired state."""

    WIDEIP_PRESENT_CNAME_MISSING = "wideip_present_cname_missing"
    """WideIP exists in F5 but the corresponding CNAME is absent in Infoblox."""

    CNAME_PRESENT_WIDEIP_MISSING = "cname_present_wideip_missing"
    """CNAME resolves but the WideIP it points at is absent.  HIGH SEVERITY:
    DNS resolves to nothing — requests return NXDOMAIN or timeout."""

    PENDING_DELETE_STILL_PRESENT = "pending_delete_still_present"
    """Object marked PENDING_DELETE in MSSQL but still present in target system."""


# Severity map: HIGH categories trigger elevated alert signals.
_HIGH_SEVERITY: frozenset[DriftCategory] = frozenset(
    {DriftCategory.CNAME_PRESENT_WIDEIP_MISSING}
)


# ---------------------------------------------------------------------------
# DriftItem dataclass
# ---------------------------------------------------------------------------


@dataclass
class DriftItem:
    """A single detected drift discrepancy."""

    wip_fqdn: str
    """The WideIP FQDN that is the root subject of this drift item."""

    object_type: str
    """Object type: 'monitor', 'pool', 'pool_member', 'wideip', 'cname'."""

    device_id: str
    """The target F5 device or Infoblox grid-master identifier."""

    category: DriftCategory
    """Classification of the discrepancy."""

    db_state: dict[str, Any] | None
    """Desired/tracked state from MSSQL.  None if the object is in_f5_not_in_db."""

    actual_state: dict[str, Any] | None
    """Current state read from F5/Infoblox.  None if object is absent."""

    diff_summary: str
    """Human-readable description of the discrepancy for structured logging."""

    severity: str = field(init=False)

    def __post_init__(self) -> None:
        self.severity = "HIGH" if self.category in _HIGH_SEVERITY else "NORMAL"


# ---------------------------------------------------------------------------
# Client Protocol stubs (for type safety without hard-coupling to clients/)
# ---------------------------------------------------------------------------


@runtime_checkable
class F5GTMClientProtocol(Protocol):
    """Minimal interface the Reconciler expects from an F5 GTM client."""

    async def get_wideip(self, fqdn: str) -> dict[str, Any] | None:
        """Return the WideIP config dict, or None if absent."""
        ...

    async def get_pool(self, pool_name: str) -> dict[str, Any] | None:
        """Return pool config dict, or None if absent."""
        ...

    async def get_monitor(self, monitor_name: str) -> dict[str, Any] | None:
        """Return monitor config dict, or None if absent."""
        ...


@runtime_checkable
class InfobloxClientProtocol(Protocol):
    """Minimal interface the Reconciler expects from an Infoblox client."""

    async def get_cname(self, fqdn: str) -> dict[str, Any] | None:
        """Return CNAME record dict, or None if absent."""
        ...


# ---------------------------------------------------------------------------
# Reconciler
# ---------------------------------------------------------------------------


class Reconciler:
    """
    Read-only drift sweep between MSSQL and F5/Infoblox.

    Parameters
    ----------
    db_conn_factory:
        Zero-argument callable returning an open ``pyodbc.Connection``.
    f5_clients:
        Mapping of ``device_id → F5GTMClient`` (or any object satisfying
        ``F5GTMClientProtocol``).  Injected; not imported from clients/.
    infoblox_client:
        Object satisfying ``InfobloxClientProtocol``.  Injected.
    page_size:
        Number of ``managed_objects`` rows to fetch per page.  Keeps
        memory footprint bounded for large estates.
    write_enabled:
        **Must be False.**  Passing True raises immediately (D-10).
    """

    def __init__(
        self,
        db_conn_factory: Any,
        f5_clients: dict[str, Any],
        infoblox_client: Any,
        page_size: int = 100,
        write_enabled: bool = False,
    ) -> None:
        if write_enabled:
            raise ValueError(
                "Reconciler write mode is not implemented. D-10 is absolute. "
                "The reconciler is permanently report-only. "
                "See §1 D-10 and CLAUDE.md."
            )

        self._db_conn_factory = db_conn_factory
        self._f5_clients = f5_clients
        self._infoblox_client = infoblox_client
        self._page_size = page_size
        # write_enabled is False and stored only for introspection.
        self._write_enabled: bool = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run_sweep(
        self,
        device_id: str | None = None,
    ) -> list[DriftItem]:
        """
        Paginated sweep of managed objects.

        Reads from ``managed_objects``, compares each row against the
        corresponding object in F5 and/or Infoblox, and collects
        ``DriftItem`` records for any discrepancy found.

        Parameters
        ----------
        device_id:
            If provided, restrict the sweep to a single F5 device.
            Useful for targeted re-checks after a device returns from
            maintenance.

        Returns
        -------
        list[DriftItem]
            All drift items found.  Empty list means no drift detected.
            Never raises on a single comparison failure — logs and
            continues to the next object.
        """
        all_drift: list[DriftItem] = []
        offset = 0

        conn = self._db_conn_factory()
        try:
            while True:
                page = _fetch_managed_objects_page(
                    conn,
                    page_size=self._page_size,
                    offset=offset,
                    device_id=device_id,
                )
                if not page:
                    break

                for db_obj in page:
                    try:
                        drift_items = await self._compare_object(db_obj)
                        all_drift.extend(drift_items)
                    except Exception as exc:
                        log.error(
                            "reconciler_comparison_error",
                            wip_fqdn=db_obj.get("wip_fqdn"),
                            object_type=db_obj.get("object_type"),
                            error=str(exc),
                        )

                offset += self._page_size

        finally:
            try:
                conn.close()
            except Exception:
                pass

        report = self._generate_report(all_drift)
        log.info(
            "reconciler_sweep_complete",
            device_id=device_id or "all",
            total_drift_items=len(all_drift),
            **{f"cat_{k}": v for k, v in report["by_category"].items()},
        )
        return all_drift

    def _detect_drift(
        self,
        db_obj: dict[str, Any],
        actual: dict[str, Any] | None,
    ) -> DriftCategory | None:
        """
        Pure function: compare a desired DB state to an actual target-system
        state and return the appropriate DriftCategory, or None if no drift.

        Parameters
        ----------
        db_obj:
            A row from ``managed_objects`` as a plain dict.
        actual:
            The current state dict read from F5 or Infoblox, or None if
            the object was not found.

        Returns
        -------
        DriftCategory | None
            None  → no drift; states match.
            Category → type of drift detected.

        Notes
        -----
        This function is deliberately pure (no I/O) so it can be tested
        exhaustively in isolation.
        """
        status = db_obj.get("status", "ACTIVE")

        # Object marked for deletion but still present.
        if status == "PENDING_DELETE" and actual is not None:
            return DriftCategory.PENDING_DELETE_STILL_PRESENT

        # Object known to DB but absent from target system.
        if status == "ACTIVE" and actual is None:
            return DriftCategory.IN_DB_MISSING_IN_F5

        # Both present — check attributes.
        if actual is not None:
            import json
            desired_raw = db_obj.get("desired_state_json")
            if desired_raw:
                try:
                    desired = json.loads(desired_raw) if isinstance(desired_raw, str) else desired_raw
                except (ValueError, TypeError):
                    desired = {}
            else:
                desired = {}

            if not _states_match(desired, actual):
                return DriftCategory.ATTRIBUTES_DIFFER

        # No drift.
        return None

    def _generate_report(self, items: list[DriftItem]) -> dict[str, Any]:
        """
        Summarize drift items by category and severity.

        Intended for structured logging and Prometheus metrics.

        Returns
        -------
        dict with keys:
            total        : int — total items
            by_category  : dict[category_value, count]
            by_severity  : dict["HIGH"|"NORMAL", count]
            high_severity: list[dict] — abbreviated detail for HIGH items
        """
        by_category: dict[str, int] = {}
        by_severity: dict[str, int] = {"HIGH": 0, "NORMAL": 0}
        high_severity: list[dict[str, Any]] = []

        for item in items:
            cat_key = item.category.value
            by_category[cat_key] = by_category.get(cat_key, 0) + 1
            by_severity[item.severity] = by_severity.get(item.severity, 0) + 1

            if item.severity == "HIGH":
                high_severity.append(
                    {
                        "wip_fqdn": item.wip_fqdn,
                        "object_type": item.object_type,
                        "device_id": item.device_id,
                        "category": cat_key,
                        "diff_summary": item.diff_summary,
                    }
                )

        return {
            "total": len(items),
            "by_category": by_category,
            "by_severity": by_severity,
            "high_severity": high_severity,
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _compare_object(self, db_obj: dict[str, Any]) -> list[DriftItem]:
        """
        Compare a single managed_object row against live target state.

        Returns a list because some FQDN comparisons produce more than one
        DriftItem (e.g. WideIP present but CNAME missing = two items of
        different types).
        """
        items: list[DriftItem] = []
        object_type = db_obj.get("object_type", "")
        wip_fqdn = db_obj.get("wip_fqdn", "")
        device_id = db_obj.get("target_device", "")
        object_key = db_obj.get("object_key", "")

        if object_type in ("wideip", "pool", "monitor", "pool_member"):
            actual = await self._fetch_f5_object(device_id, object_type, object_key)
            category = self._detect_drift(db_obj, actual)

            if category is not None:
                items.append(
                    DriftItem(
                        wip_fqdn=wip_fqdn,
                        object_type=object_type,
                        device_id=device_id,
                        category=category,
                        db_state=_parse_desired_state(db_obj),
                        actual_state=actual,
                        diff_summary=_build_diff_summary(
                            category, db_obj, actual
                        ),
                    )
                )

            # Cross-check: if the WideIP is present in F5, ensure CNAME exists.
            if object_type == "wideip" and actual is not None:
                cname_actual = await self._fetch_infoblox_cname(wip_fqdn)
                if cname_actual is None:
                    items.append(
                        DriftItem(
                            wip_fqdn=wip_fqdn,
                            object_type="cname",
                            device_id=device_id,
                            category=DriftCategory.WIDEIP_PRESENT_CNAME_MISSING,
                            db_state=_parse_desired_state(db_obj),
                            actual_state=None,
                            diff_summary=(
                                f"WideIP '{object_key}' exists in F5 but "
                                f"CNAME '{wip_fqdn}' is absent in Infoblox."
                            ),
                        )
                    )

        elif object_type == "cname":
            cname_actual = await self._fetch_infoblox_cname(object_key)
            category = self._detect_drift(db_obj, cname_actual)

            if category is not None:
                items.append(
                    DriftItem(
                        wip_fqdn=wip_fqdn,
                        object_type="cname",
                        device_id=device_id,
                        category=category,
                        db_state=_parse_desired_state(db_obj),
                        actual_state=cname_actual,
                        diff_summary=_build_diff_summary(
                            category, db_obj, cname_actual
                        ),
                    )
                )

            # Cross-check: if the CNAME exists but WideIP is absent → HIGH.
            if cname_actual is not None:
                wideip_actual = await self._fetch_f5_object(
                    device_id, "wideip", wip_fqdn
                )
                if wideip_actual is None:
                    items.append(
                        DriftItem(
                            wip_fqdn=wip_fqdn,
                            object_type="wideip",
                            device_id=device_id,
                            category=DriftCategory.CNAME_PRESENT_WIDEIP_MISSING,
                            db_state=_parse_desired_state(db_obj),
                            actual_state=None,
                            diff_summary=(
                                f"CNAME '{object_key}' exists in Infoblox but "
                                f"WideIP '{wip_fqdn}' is absent in F5 (device "
                                f"'{device_id}'). DNS resolves to nothing."
                            ),
                        )
                    )

        return items

    async def _fetch_f5_object(
        self,
        device_id: str,
        object_type: str,
        object_key: str,
    ) -> dict[str, Any] | None:
        """Retrieve a single object from the appropriate F5 client."""
        client = self._f5_clients.get(device_id)
        if client is None:
            log.warning(
                "reconciler_no_f5_client",
                device_id=device_id,
                object_type=object_type,
                object_key=object_key,
            )
            return None

        try:
            if object_type == "wideip":
                return await client.get_wideip(object_key)
            if object_type == "pool":
                return await client.get_pool(object_key)
            if object_type == "monitor":
                return await client.get_monitor(object_key)
            # pool_member is a sub-object; no direct get method required at
            # this stage.  Log and skip.
            log.debug(
                "reconciler_unsupported_object_type",
                object_type=object_type,
                object_key=object_key,
            )
            return None
        except Exception as exc:
            log.error(
                "reconciler_f5_fetch_error",
                device_id=device_id,
                object_type=object_type,
                object_key=object_key,
                error=str(exc),
            )
            return None

    async def _fetch_infoblox_cname(self, fqdn: str) -> dict[str, Any] | None:
        """Retrieve a CNAME record from Infoblox."""
        try:
            return await self._infoblox_client.get_cname(fqdn)
        except Exception as exc:
            log.error(
                "reconciler_infoblox_fetch_error",
                fqdn=fqdn,
                error=str(exc),
            )
            return None


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _fetch_managed_objects_page(
    conn: Any,
    page_size: int,
    offset: int,
    device_id: str | None,
) -> list[dict[str, Any]]:
    """
    Fetch one page of managed_objects rows, ordered by ``last_verified_at``
    ascending (NULLs first) so the stalest objects are checked first (T-6.4).
    """
    if device_id is not None:
        sql = """
            SELECT object_id, wip_fqdn, object_type, object_key,
                   target_device, desired_state_json, last_verified_at,
                   drift_detected_at, drift_details_json,
                   owning_request_id, status
            FROM managed_objects
            WHERE status     != 'DELETED'
              AND target_device = ?
            ORDER BY last_verified_at ASC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """
        params: tuple[Any, ...] = (device_id, offset, page_size)
    else:
        sql = """
            SELECT object_id, wip_fqdn, object_type, object_key,
                   target_device, desired_state_json, last_verified_at,
                   drift_detected_at, drift_details_json,
                   owning_request_id, status
            FROM managed_objects
            WHERE status != 'DELETED'
            ORDER BY last_verified_at ASC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """
        params = (offset, page_size)

    cursor = conn.cursor()
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in rows]


def _parse_desired_state(db_obj: dict[str, Any]) -> dict[str, Any] | None:
    """Parse ``desired_state_json`` from a managed_objects row."""
    import json

    raw = db_obj.get("desired_state_json")
    if raw is None:
        return None
    try:
        return json.loads(raw) if isinstance(raw, str) else raw
    except (ValueError, TypeError):
        return None


def _states_match(desired: dict[str, Any], actual: dict[str, Any]) -> bool:
    """
    Return True if every key in ``desired`` is present in ``actual`` with
    an equal value.

    Extra keys in ``actual`` (added by the target system) are ignored —
    we compare only the fields we care about, not the full API response.

    This is a shallow comparison.  Nested dicts are compared by equality.
    """
    for key, expected_value in desired.items():
        if key not in actual:
            return False
        if actual[key] != expected_value:
            return False
    return True


def _build_diff_summary(
    category: DriftCategory,
    db_obj: dict[str, Any],
    actual: dict[str, Any] | None,
) -> str:
    """Build a concise human-readable diff summary for a DriftItem."""
    import json

    fqdn = db_obj.get("wip_fqdn", "?")
    obj_type = db_obj.get("object_type", "?")
    obj_key = db_obj.get("object_key", "?")

    if category == DriftCategory.IN_DB_MISSING_IN_F5:
        return (
            f"{obj_type} '{obj_key}' (fqdn='{fqdn}') tracked in MSSQL "
            f"but absent from target system."
        )
    if category == DriftCategory.IN_F5_NOT_IN_DB:
        return (
            f"{obj_type} '{obj_key}' exists in target system but has no "
            f"corresponding MSSQL record."
        )
    if category == DriftCategory.ATTRIBUTES_DIFFER:
        desired = _parse_desired_state(db_obj) or {}
        diffs = []
        if actual:
            for k, v in desired.items():
                if k in actual and actual[k] != v:
                    diffs.append(f"{k}: desired={v!r} actual={actual[k]!r}")
        diff_str = "; ".join(diffs) if diffs else "see db_state vs actual_state"
        return f"{obj_type} '{obj_key}' attribute mismatch: {diff_str}"
    if category == DriftCategory.PENDING_DELETE_STILL_PRESENT:
        return (
            f"{obj_type} '{obj_key}' is marked PENDING_DELETE in MSSQL "
            f"but still present in the target system."
        )
    # Composite categories handled elsewhere.
    return f"{category.value} for {obj_type} '{obj_key}'"
