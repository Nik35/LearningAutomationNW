"""
Operational status snapshot for the GTM automation service (T-7.5).

``build_status_snapshot`` aggregates the full system health picture into a
single dict suitable for a GET /ops/status response.  On-call engineers
should be able to assess the system state in one request.

What is included
----------------
- Kill switch and dry-run flags
- Delete cap usage for the current window
- Per-device: breaker state, semaphore slots held, queue depth
- Global remediation queue depth (from Redis)
- NEEDS_ATTENTION count (from MSSQL)
- Redis memory headroom (from Redis INFO memory)

Notes
-----
This function reads from Redis and the database.  It is intentionally
read-only — it must never modify any state.

The ``db_session`` parameter is typed as Any to avoid coupling this module
to a specific SQLAlchemy session type before the db layer is complete.
The caller is responsible for passing a session that supports::

    await db_session.execute(text("SELECT COUNT(*) FROM requests WHERE status = 'NEEDS_ATTENTION'"))

``device_ids`` is the list of known F5 device identifiers.  The caller
reads this from settings.

Redis keys read (must match the coordination layer exactly):
    breaker:{device_id}:state   → "closed" | "half_open" | "open"
                                  Defined in app/coordination/breaker.py.
    sem:{device_id}             → Hash; HLEN gives slots held.
                                  Defined in app/coordination/semaphore.py.
    queue_depth:{device_id}     → integer string.
                                  TODO: confirm key name with the coordination
                                  agent once the queue-depth writer is built.
    remediation:depth           → integer string.
                                  TODO: confirm key name with the recovery agent.
"""

from __future__ import annotations

from typing import Any

from app.core.logging import get_logger
from app.ops.controls import OperationalControls

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Redis key templates for coordination primitives.
# These are READ-ONLY references; the coordination layer owns writes.
#
# Key names must match those used by the coordination agent:
#   breaker.py  → "breaker:{device_id}:state"
#   semaphore.py → "sem:{device_id}"  (a Hash; use HLEN for slot count)
#
# Keys marked TODO need to be confirmed with the agent building those
# writers before this function can return live values.
# ---------------------------------------------------------------------------
_BREAKER_STATE_KEY = "breaker:{device_id}:state"       # matches coordination/breaker.py
_SEMAPHORE_KEY = "sem:{device_id}"                      # Hash; matches coordination/semaphore.py
_QUEUE_DEPTH_KEY = "queue_depth:{device_id}"            # TODO: confirm with coordination agent
_REMEDIATION_DEPTH_KEY = "remediation:depth"            # TODO: confirm with recovery agent

# Mapping from string state names to numeric codes used in Prometheus.
_BREAKER_STATE_CODES: dict[str, int] = {
    "closed": 0,
    "half_open": 1,
    "open": 2,
}


async def build_status_snapshot(
    redis_client: Any,
    controls: OperationalControls,
    device_ids: list[str],
    db_session: Any | None = None,
) -> dict[str, Any]:
    """
    Build a complete operational status snapshot.

    Parameters
    ----------
    redis_client:
        Open async Redis connection/pool.
    controls:
        Initialised ``OperationalControls`` instance.
    device_ids:
        Ordered list of device identifiers to include in per-device stats.
    db_session:
        Optional async SQLAlchemy session.  When supplied, queries MSSQL
        for the NEEDS_ATTENTION count.  When None, that field is omitted.

    Returns
    -------
    dict
        JSON-serialisable dict suitable for direct use as an API response
        body.  Shape::

            {
                "controls": {
                    "kill_switch": bool,
                    "dry_run": bool,
                    "delete_window": {
                        "window_seconds": int,
                        "cap": int,
                        "current_count": int,
                        "allowed": bool
                    }
                },
                "devices": {
                    "<device_id>": {
                        "breaker_state": "closed" | "half_open" | "open" | "unknown",
                        "breaker_code": 0 | 1 | 2 | null,
                        "semaphore_slots_held": int,
                        "queue_depth": int
                    },
                    ...
                },
                "remediation_queue_depth": int,
                "needs_attention_count": int | null,
                "redis_memory": {
                    "used_bytes": int,
                    "max_bytes": int | null,
                    "headroom_bytes": int | null,
                    "headroom_pct": float | null
                }
            }
    """
    # Gather controls state (kill switch, dry run, delete cap).
    controls_state = await controls.get_all_states()

    # Per-device stats — pipeline all reads in one round trip.
    devices: dict[str, Any] = {}
    if device_ids:
        pipe = redis_client.pipeline()
        for device_id in device_ids:
            # breaker state: plain string key "breaker:{device_id}:state"
            pipe.get(_BREAKER_STATE_KEY.format(device_id=device_id))
            # semaphore slots held: Hash length of "sem:{device_id}"
            pipe.hlen(_SEMAPHORE_KEY.format(device_id=device_id))
            # queue depth: plain string key (key name TBC with coordination agent)
            pipe.get(_QUEUE_DEPTH_KEY.format(device_id=device_id))
        results = await pipe.execute()

        for idx, device_id in enumerate(device_ids):
            base = idx * 3
            raw_breaker = results[base]
            slots_held = results[base + 1]   # HLEN returns int directly
            raw_queue = results[base + 2]

            breaker_name: str = (
                raw_breaker.decode() if isinstance(raw_breaker, bytes)
                else raw_breaker
            ) if raw_breaker is not None else "unknown"

            devices[device_id] = {
                "breaker_state": breaker_name,
                "breaker_code": _BREAKER_STATE_CODES.get(breaker_name),
                "semaphore_slots_held": slots_held if slots_held is not None else 0,
                "queue_depth": int(raw_queue) if raw_queue else 0,
            }

    # Remediation queue depth.
    raw_rem = await redis_client.get(_REMEDIATION_DEPTH_KEY)
    remediation_depth = int(raw_rem) if raw_rem else 0

    # NEEDS_ATTENTION count from MSSQL (optional; skip if no session).
    needs_attention_count: int | None = None
    if db_session is not None:
        try:
            from sqlalchemy import text  # deferred to avoid top-level coupling

            result = await db_session.execute(
                text(
                    "SELECT COUNT(*) FROM requests WHERE status = 'NEEDS_ATTENTION'"
                )
            )
            needs_attention_count = result.scalar_one()
        except Exception as exc:  # noqa: BLE001 — status must not crash
            log.warning("needs_attention_query_failed", error=str(exc))

    # Redis memory headroom.
    redis_memory: dict[str, Any] = {}
    try:
        mem_info: dict[str, Any] = await redis_client.info("memory")
        used = int(mem_info.get("used_memory", 0))
        maxmem_raw = mem_info.get("maxmemory")
        maxmem: int | None = int(maxmem_raw) if maxmem_raw and int(maxmem_raw) > 0 else None

        headroom_bytes: int | None = None
        headroom_pct: float | None = None
        if maxmem is not None:
            headroom_bytes = maxmem - used
            headroom_pct = round((headroom_bytes / maxmem) * 100, 2)

        redis_memory = {
            "used_bytes": used,
            "max_bytes": maxmem,
            "headroom_bytes": headroom_bytes,
            "headroom_pct": headroom_pct,
        }
    except Exception as exc:  # noqa: BLE001 — status must not crash
        log.warning("redis_memory_info_failed", error=str(exc))
        redis_memory = {
            "used_bytes": None,
            "max_bytes": None,
            "headroom_bytes": None,
            "headroom_pct": None,
        }

    snapshot: dict[str, Any] = {
        "controls": controls_state,
        "devices": devices,
        "remediation_queue_depth": remediation_depth,
        "needs_attention_count": needs_attention_count,
        "redis_memory": redis_memory,
    }

    log.debug("status_snapshot_built", device_count=len(devices))
    return snapshot
