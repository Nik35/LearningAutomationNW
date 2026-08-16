"""
app/clients/f5/gtm.py
=====================
F5 GTM (BIG-IP DNS) operations via iControl REST.

This is the only file other application code should import from this package.
Every public method implements the **read → compare → act → no-op if identical**
pattern required by §3.3 of the implementation plan.

API shapes come exclusively from ``docs/api-research-findings.md``.
No field name is invented here.

Key invariants
--------------
- Every ``ensure_*`` reads first.  A write only happens when the desired
  state differs from the current state.
- Timeouts are raised as ``F5TimeoutError``.  Callers must read back to
  determine actual state; blind-retry is never performed here.
- Delete of an already-absent object returns ``action="not_found"``; it
  does NOT raise an exception.
- ``pre_state`` is always captured before any mutation so that rollback has
  correct information.

Path encoding (from api-research-findings.md)
--------------------------------------------
The partition separator in iControl REST URLs is ``~``::

    /mgmt/tm/gtm/wideip/a/~Common~my.fqdn.example.com
    /mgmt/tm/gtm/pool/a/~Common~my-pool
    /mgmt/tm/gtm/monitor/bigip/~Common~my-monitor
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import httpx

from app.clients.f5.auth import F5TokenManager
from app.clients.f5.session import F5Session

if TYPE_CHECKING:
    from app.coordination.breaker import DeviceCircuitBreaker
    from app.coordination.ratelimit import DeviceTokenBucket

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class F5Error(Exception):
    """Base class for all F5 client errors."""


class F5NotFoundError(F5Error):
    """The requested resource does not exist on the device (HTTP 404)."""


class F5ConflictError(F5Error):
    """
    A conflicting resource already exists (HTTP 409), or the request
    conflicts with the current state of the device.
    """


class F5TimeoutError(F5Error):
    """
    A request to the F5 device timed out.

    **The outcome is unknown.**  Callers must read back to determine actual
    state before deciding what to do next.  Blind-retry is forbidden (see
    failure-matrix row 7 in the implementation plan).

    Attributes
    ----------
    operation:
        Human-readable description of the operation that timed out.
    path:
        The URL path that was targeted.
    """

    def __init__(self, message: str, operation: str, path: str) -> None:
        super().__init__(message)
        self.operation = operation
        self.path = path


class F5ServerError(F5Error):
    """The F5 device returned a 5xx response."""


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

ActionTaken = Literal["created", "updated", "no_op", "deleted", "not_found"]


@dataclass
class OperationResult:
    """
    Outcome of a single GTM operation.

    Attributes
    ----------
    action:
        What the client actually did.
    pre_state:
        The state of the object *before* this call.  ``None`` means the
        object did not exist before the call.
    post_state:
        The state of the object *after* this call.  ``None`` means the
        object was deleted or was not found.
    """

    action: ActionTaken
    pre_state: dict | None  # type: ignore[type-arg]
    post_state: dict | None  # type: ignore[type-arg]


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


def _monitor_path(monitor_type: str, partition: str, name: str) -> str:
    """
    Build the iControl REST path for a GTM monitor resource.

    Example: ``/mgmt/tm/gtm/monitor/bigip/~Common~my-monitor``
    """
    return f"/mgmt/tm/gtm/monitor/{monitor_type}/~{partition}~{name}"


def _pool_path(pool_type: str, partition: str, name: str) -> str:
    """
    Build the iControl REST path for a GTM pool resource.

    Example: ``/mgmt/tm/gtm/pool/a/~Common~my-pool``
    """
    return f"/mgmt/tm/gtm/pool/{pool_type}/~{partition}~{name}"


def _pool_members_path(pool_type: str, partition: str, pool_name: str) -> str:
    """
    Build the iControl REST path for the members sub-collection of a GTM pool.

    Example: ``/mgmt/tm/gtm/pool/a/~Common~my-pool/members``
    """
    return f"/mgmt/tm/gtm/pool/{pool_type}/~{partition}~{pool_name}/members"


def _wideip_path(wideip_type: str, partition: str, name: str) -> str:
    """
    Build the iControl REST path for a GTM WideIP resource.

    Example: ``/mgmt/tm/gtm/wideip/a/~Common~my.fqdn.example.com``
    """
    return f"/mgmt/tm/gtm/wideip/{wideip_type}/~{partition}~{name}"


def _resource_path(resource_type: str, partition: str, name: str) -> str:
    """
    Generic helper used in tests and for WideIP paths.

    ``resource_type`` is the full sub-path after ``/mgmt/tm/gtm/``, e.g.
    ``wideip/a``.

    Example: ``/mgmt/tm/gtm/wideip/a/~Common~my.fqdn.example.com``
    """
    return f"/mgmt/tm/gtm/{resource_type}/~{partition}~{name}"


def _configs_equal(desired: dict, current: dict) -> bool:  # type: ignore[type-arg]
    """
    Compare fields present in ``desired`` against the corresponding fields
    in ``current``.

    Only keys present in ``desired`` are compared; extra keys returned by
    the F5 API (e.g. ``kind``, ``selfLink``, ``generation``) are ignored.
    Values are compared after serialising to JSON so that nested structures
    and type differences (int vs float) are handled consistently.
    """
    for key, desired_value in desired.items():
        current_value = current.get(key)
        if json.dumps(desired_value, sort_keys=True) != json.dumps(
            current_value, sort_keys=True
        ):
            return False
    return True


# ---------------------------------------------------------------------------
# GTM client
# ---------------------------------------------------------------------------


class F5GTMClient:
    """
    High-level interface for GTM/DNS objects on a single F5 BIG-IP device.

    All methods are idempotent:
    - ``get_*``    returns current state or ``None`` / empty list
    - ``ensure_*`` reads, compares, writes only when different
    - ``delete_*`` returns ``not_found`` rather than raising when absent

    Parameters
    ----------
    session:
        The ``F5Session`` for the target device.
    token_manager:
        The ``F5TokenManager`` for the target device.
    """

    def __init__(
        self,
        session: F5Session,
        token_manager: F5TokenManager,
        token_bucket: "DeviceTokenBucket | None" = None,
        circuit_breaker: "DeviceCircuitBreaker | None" = None,
    ) -> None:
        self._session = session
        self._token_manager = token_manager
        self._token_bucket = token_bucket      # P-2/P-3: awaiting T-0.7
        self._circuit_breaker = circuit_breaker  # P-10: awaiting T-0.6/T-0.7

    # -----------------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------------

    async def _authed_headers(self) -> dict[str, str]:
        """Return the standard JSON + auth headers."""
        return await self._token_manager.inject_auth(
            {"Content-Type": "application/json", "Accept": "application/json"}
        )

    async def _consume_token(self) -> None:
        """Consume one token from the rate-limit bucket before each F5 call."""
        if self._token_bucket is not None:
            allowed = await self._token_bucket.consume(1)
            if not allowed:
                raise F5Error("Rate limit bucket exhausted — request rejected before F5 call")

    async def _record_outcome(self, latency_ms: float, *, timed_out: bool = False, failed: bool = False) -> None:
        """Feed call outcome into the circuit breaker."""
        if self._circuit_breaker is None:
            return
        if timed_out:
            await self._circuit_breaker.record_timeout()
        elif failed:
            await self._circuit_breaker.record_failure(latency_ms)
        else:
            await self._circuit_breaker.record_success(latency_ms)

    async def _get(self, path: str) -> dict | None:  # type: ignore[type-arg]
        """
        GET ``path``.  Returns the parsed JSON body on 200, ``None`` on 404.

        Raises
        ------
        F5TimeoutError
            On ``httpx.TimeoutException``.
        F5ServerError
            On 5xx responses.
        F5Error
            On any other non-200/404 response.
        """
        await self._consume_token()
        headers = await self._authed_headers()
        t0 = time.monotonic()
        try:
            response = await self._session.request("GET", path, headers=headers)
        except httpx.TimeoutException as exc:
            await self._record_outcome(0, timed_out=True)
            raise F5TimeoutError(
                f"GET {path} timed out: {exc}", operation="GET", path=path
            ) from exc

        latency_ms = (time.monotonic() - t0) * 1000
        is_error = response.status_code >= 500
        await self._record_outcome(latency_ms, failed=is_error)

        if response.status_code == 404:
            return None
        if 500 <= response.status_code < 600:
            raise F5ServerError(
                f"GET {path} returned {response.status_code}: {response.text}"
            )
        if response.status_code != 200:
            raise F5Error(
                f"GET {path} returned unexpected status {response.status_code}: "
                f"{response.text}"
            )
        return response.json()  # type: ignore[no-any-return]

    async def _post(self, path: str, payload: dict) -> dict:  # type: ignore[type-arg]
        """POST ``payload`` to ``path``.  Returns the created resource body."""
        await self._consume_token()
        headers = await self._authed_headers()
        t0 = time.monotonic()
        try:
            response = await self._session.request(
                "POST", path, headers=headers, json=payload
            )
        except httpx.TimeoutException as exc:
            await self._record_outcome(0, timed_out=True)
            raise F5TimeoutError(
                f"POST {path} timed out: {exc}", operation="POST", path=path
            ) from exc

        latency_ms = (time.monotonic() - t0) * 1000
        is_error = response.status_code >= 500
        await self._record_outcome(latency_ms, failed=is_error)

        if response.status_code == 409:
            raise F5ConflictError(
                f"POST {path} returned 409: {response.text}"
            )
        if 500 <= response.status_code < 600:
            raise F5ServerError(
                f"POST {path} returned {response.status_code}: {response.text}"
            )
        if response.status_code not in (200, 201):
            raise F5Error(
                f"POST {path} returned unexpected status {response.status_code}: "
                f"{response.text}"
            )
        return response.json()  # type: ignore[no-any-return]

    async def _patch(self, path: str, payload: dict) -> dict:  # type: ignore[type-arg]
        """PATCH ``payload`` onto ``path``.  Returns the updated resource body."""
        await self._consume_token()
        headers = await self._authed_headers()
        t0 = time.monotonic()
        try:
            response = await self._session.request(
                "PATCH", path, headers=headers, json=payload
            )
        except httpx.TimeoutException as exc:
            await self._record_outcome(0, timed_out=True)
            raise F5TimeoutError(
                f"PATCH {path} timed out: {exc}", operation="PATCH", path=path
            ) from exc

        latency_ms = (time.monotonic() - t0) * 1000
        await self._record_outcome(latency_ms, failed=response.status_code >= 500)

        if 500 <= response.status_code < 600:
            raise F5ServerError(
                f"PATCH {path} returned {response.status_code}: {response.text}"
            )
        if response.status_code != 200:
            raise F5Error(
                f"PATCH {path} returned unexpected status {response.status_code}: "
                f"{response.text}"
            )
        return response.json()  # type: ignore[no-any-return]

    async def _put(self, path: str, payload: dict) -> dict:  # type: ignore[type-arg]
        """PUT ``payload`` to ``path``.  Returns the updated resource body."""
        await self._consume_token()
        headers = await self._authed_headers()
        t0 = time.monotonic()
        try:
            response = await self._session.request(
                "PUT", path, headers=headers, json=payload
            )
        except httpx.TimeoutException as exc:
            await self._record_outcome(0, timed_out=True)
            raise F5TimeoutError(
                f"PUT {path} timed out: {exc}", operation="PUT", path=path
            ) from exc

        latency_ms = (time.monotonic() - t0) * 1000
        await self._record_outcome(latency_ms, failed=response.status_code >= 500)

        if 500 <= response.status_code < 600:
            raise F5ServerError(
                f"PUT {path} returned {response.status_code}: {response.text}"
            )
        if response.status_code != 200:
            raise F5Error(
                f"PUT {path} returned unexpected status {response.status_code}: "
                f"{response.text}"
            )
        return response.json()  # type: ignore[no-any-return]

    async def _delete(self, path: str) -> None:
        """
        DELETE ``path``.

        Returns ``None`` on 200/204.  Does NOT raise on 404 — callers handle
        that via the return value of ``delete_*`` methods.
        """
        await self._consume_token()
        headers = await self._authed_headers()
        t0 = time.monotonic()
        try:
            response = await self._session.request("DELETE", path, headers=headers)
        except httpx.TimeoutException as exc:
            await self._record_outcome(0, timed_out=True)
            raise F5TimeoutError(
                f"DELETE {path} timed out: {exc}", operation="DELETE", path=path
            ) from exc

        latency_ms = (time.monotonic() - t0) * 1000
        await self._record_outcome(latency_ms, failed=response.status_code >= 500)

        if response.status_code == 404:
            raise F5NotFoundError(f"DELETE {path}: resource not found")
        if 500 <= response.status_code < 600:
            raise F5ServerError(
                f"DELETE {path} returned {response.status_code}: {response.text}"
            )
        if response.status_code not in (200, 204):
            raise F5Error(
                f"DELETE {path} returned unexpected status {response.status_code}: "
                f"{response.text}"
            )

    # -----------------------------------------------------------------------
    # Monitors
    # -----------------------------------------------------------------------

    async def get_monitor(
        self,
        monitor_type: str,
        name: str,
        partition: str = "Common",
    ) -> dict | None:  # type: ignore[type-arg]
        """
        GET the named GTM monitor.

        Returns the resource dict on success, ``None`` on 404.

        Parameters
        ----------
        monitor_type:
            The monitor sub-type, e.g. ``"bigip"``, ``"http"``, ``"https"``.
        name:
            The monitor name.
        partition:
            BIG-IP partition.  Defaults to ``"Common"``.
        """
        path = _monitor_path(monitor_type, partition, name)
        return await self._get(path)

    async def ensure_monitor(
        self,
        monitor_type: str,
        name: str,
        config: dict,  # type: ignore[type-arg]
        partition: str = "Common",
    ) -> OperationResult:
        """
        Idempotent create-or-update for a GTM monitor.

        Algorithm:
          1. GET current state → ``pre_state``
          2. If absent → POST to create
          3. If present and identical → no-op (no HTTP write)
          4. If present and different → PATCH with desired config

        Parameters
        ----------
        monitor_type:
            e.g. ``"bigip"``
        name:
            Monitor name.
        config:
            Desired configuration fields.  Only keys present here are
            compared against the current state.  Must not include ``name``
            or ``partition`` — those are in the URL.
        partition:
            BIG-IP partition.  Defaults to ``"Common"``.
        """
        path = _monitor_path(monitor_type, partition, name)
        collection_path = f"/mgmt/tm/gtm/monitor/{monitor_type}"

        pre_state = await self._get(path)

        if pre_state is None:
            # Object does not exist — create it.
            create_payload = {"name": name, "partition": partition, **config}
            post_state = await self._post(collection_path, create_payload)
            return OperationResult(action="created", pre_state=None, post_state=post_state)

        if _configs_equal(config, pre_state):
            return OperationResult(action="no_op", pre_state=pre_state, post_state=pre_state)

        # Object exists but differs — update.
        post_state = await self._patch(path, config)
        return OperationResult(action="updated", pre_state=pre_state, post_state=post_state)

    async def delete_monitor(
        self,
        monitor_type: str,
        name: str,
        partition: str = "Common",
    ) -> OperationResult:
        """
        Idempotent delete for a GTM monitor.

        Returns ``action="not_found"`` when the monitor is already absent
        (no error raised).
        """
        path = _monitor_path(monitor_type, partition, name)
        pre_state = await self._get(path)
        if pre_state is None:
            return OperationResult(action="not_found", pre_state=None, post_state=None)

        await self._delete(path)
        return OperationResult(action="deleted", pre_state=pre_state, post_state=None)

    # -----------------------------------------------------------------------
    # Pools
    # -----------------------------------------------------------------------

    async def get_pool(
        self,
        pool_type: str,
        name: str,
        partition: str = "Common",
    ) -> dict | None:  # type: ignore[type-arg]
        """
        GET the named GTM pool.  Returns ``None`` on 404.

        Parameters
        ----------
        pool_type:
            Pool record type, e.g. ``"a"``.
        name:
            Pool name.
        partition:
            BIG-IP partition.  Defaults to ``"Common"``.
        """
        path = _pool_path(pool_type, partition, name)
        return await self._get(path)

    async def ensure_pool(
        self,
        pool_type: str,
        name: str,
        config: dict,  # type: ignore[type-arg]
        partition: str = "Common",
    ) -> OperationResult:
        """
        Idempotent create-or-update for a GTM pool.

        Same read → compare → act → no-op pattern as ``ensure_monitor``.
        """
        path = _pool_path(pool_type, partition, name)
        collection_path = f"/mgmt/tm/gtm/pool/{pool_type}"

        pre_state = await self._get(path)

        if pre_state is None:
            create_payload = {"name": name, "partition": partition, **config}
            post_state = await self._post(collection_path, create_payload)
            return OperationResult(action="created", pre_state=None, post_state=post_state)

        if _configs_equal(config, pre_state):
            return OperationResult(action="no_op", pre_state=pre_state, post_state=pre_state)

        post_state = await self._patch(path, config)
        return OperationResult(action="updated", pre_state=pre_state, post_state=post_state)

    async def delete_pool(
        self,
        pool_type: str,
        name: str,
        partition: str = "Common",
    ) -> OperationResult:
        """Idempotent delete for a GTM pool.  Returns ``not_found`` if absent."""
        path = _pool_path(pool_type, partition, name)
        pre_state = await self._get(path)
        if pre_state is None:
            return OperationResult(action="not_found", pre_state=None, post_state=None)

        await self._delete(path)
        return OperationResult(action="deleted", pre_state=pre_state, post_state=None)

    # -----------------------------------------------------------------------
    # Pool members
    # -----------------------------------------------------------------------

    async def get_pool_members(
        self,
        pool_type: str,
        pool_name: str,
        partition: str = "Common",
    ) -> list[dict]:  # type: ignore[type-arg]
        """
        Return the members of the named GTM pool as a list.

        Returns an empty list if the pool does not exist or has no members.

        The sub-collection response from iControl REST wraps items in an
        ``"items"`` key.  If the key is absent (empty collection), an empty
        list is returned.
        """
        path = _pool_members_path(pool_type, partition, pool_name)
        body = await self._get(path)
        if body is None:
            return []
        return body.get("items", [])  # type: ignore[return-value]

    async def ensure_pool_members(
        self,
        pool_type: str,
        pool_name: str,
        members: list[dict],  # type: ignore[type-arg]
        partition: str = "Common",
    ) -> OperationResult:
        """
        Idempotent set of the members on a GTM pool.

        Strategy: PATCH the parent pool with the full desired ``members``
        array (from api-research-findings.md: "PATCH the parent pool with an
        updated members array").

        Reads existing members first; if the lists are equal (by JSON
        comparison after sorting by ``name``) → no-op.

        Parameters
        ----------
        pool_type:
            e.g. ``"a"``
        pool_name:
            Pool name.
        members:
            Desired members list.  Each entry is a dict with fields from
            api-research-findings.md: ``name``, ``order``, ``ratio``, etc.
        partition:
            BIG-IP partition.
        """
        pool_path = _pool_path(pool_type, partition, pool_name)
        members_path = _pool_members_path(pool_type, partition, pool_name)

        # Capture pre-state from the members sub-collection for rollback fidelity.
        members_body = await self._get(members_path)
        current_members: list[dict] = []  # type: ignore[type-arg]
        if members_body is not None:
            current_members = members_body.get("items", [])

        # Compare by serialising sorted lists to JSON.
        def _sort_key(m: dict) -> str:  # type: ignore[type-arg]
            return str(m.get("name", ""))

        current_sorted = sorted(current_members, key=_sort_key)
        desired_sorted = sorted(members, key=_sort_key)

        if json.dumps(current_sorted, sort_keys=True) == json.dumps(
            desired_sorted, sort_keys=True
        ):
            pre_state = {"items": current_members}
            return OperationResult(action="no_op", pre_state=pre_state, post_state=pre_state)

        pre_state = {"items": current_members}
        post_state = await self._patch(pool_path, {"members": members})
        return OperationResult(
            action="updated", pre_state=pre_state, post_state=post_state
        )

    async def delete_all_pool_members(
        self,
        pool_type: str,
        pool_name: str,
        partition: str = "Common",
    ) -> OperationResult:
        """
        Remove all members from a GTM pool by PATCHing with an empty list.

        Returns ``no_op`` if the pool has no members already.
        """
        pool_path = _pool_path(pool_type, partition, pool_name)
        members_path = _pool_members_path(pool_type, partition, pool_name)

        members_body = await self._get(members_path)
        current_members: list[dict] = []  # type: ignore[type-arg]
        if members_body is not None:
            current_members = members_body.get("items", [])

        if not current_members:
            return OperationResult(
                action="no_op",
                pre_state={"items": []},
                post_state={"items": []},
            )

        pre_state = {"items": current_members}
        post_state = await self._patch(pool_path, {"members": []})
        return OperationResult(action="deleted", pre_state=pre_state, post_state=post_state)

    # -----------------------------------------------------------------------
    # WideIPs
    # -----------------------------------------------------------------------

    async def get_wideip(
        self,
        wideip_type: str,
        name: str,
        partition: str = "Common",
    ) -> dict | None:  # type: ignore[type-arg]
        """
        GET the named GTM WideIP.  Returns ``None`` on 404.

        Parameters
        ----------
        wideip_type:
            WideIP record type, e.g. ``"a"``, ``"aaaa"``, ``"cname"``.
        name:
            The FQDN of the WideIP.
        partition:
            BIG-IP partition.  Defaults to ``"Common"``.
        """
        path = _wideip_path(wideip_type, partition, name)
        return await self._get(path)

    async def ensure_wideip(
        self,
        wideip_type: str,
        name: str,
        config: dict,  # type: ignore[type-arg]
        partition: str = "Common",
    ) -> OperationResult:
        """
        Idempotent create-or-update for a GTM WideIP.

        Same read → compare → act → no-op pattern as the other ``ensure_*``
        methods.

        Note: iControl REST WideIP paths use ``wideip/{type}`` not just
        ``{type}`` (unlike pool and monitor).
        """
        path = _wideip_path(wideip_type, partition, name)
        collection_path = f"/mgmt/tm/gtm/wideip/{wideip_type}"

        pre_state = await self._get(path)

        if pre_state is None:
            create_payload = {"name": name, "partition": partition, **config}
            post_state = await self._post(collection_path, create_payload)
            return OperationResult(action="created", pre_state=None, post_state=post_state)

        if _configs_equal(config, pre_state):
            return OperationResult(action="no_op", pre_state=pre_state, post_state=pre_state)

        post_state = await self._patch(path, config)
        return OperationResult(action="updated", pre_state=pre_state, post_state=post_state)

    async def delete_wideip(
        self,
        wideip_type: str,
        name: str,
        partition: str = "Common",
    ) -> OperationResult:
        """Idempotent delete for a GTM WideIP.  Returns ``not_found`` if absent."""
        path = _wideip_path(wideip_type, partition, name)
        pre_state = await self._get(path)
        if pre_state is None:
            return OperationResult(action="not_found", pre_state=None, post_state=None)

        await self._delete(path)
        return OperationResult(action="deleted", pre_state=pre_state, post_state=None)
