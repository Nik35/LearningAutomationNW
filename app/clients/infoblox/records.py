"""
app/clients/infoblox/records.py
================================
Infoblox WAPI CNAME record operations.

Confirmed API shapes (from docs/api-research-findings.md):
  Object type : ``record:cname``
  Create      : POST  /wapi/v{ver}/record:cname
  Search      : GET   /wapi/v{ver}/record:cname?name={name}&view={view}&_return_fields=...
  Update      : PUT   /wapi/v{ver}/{_ref}
  Delete      : DELETE /wapi/v{ver}/{_ref}

Only field names confirmed in api-research-findings.md are used here.
No field names are invented.

Hard rules (CLAUDE.md):
  - Every operation is idempotent: read → compare → act → no-op if identical.
  - Timeouts raise InfobloxTimeoutError.  Caller must read back before retrying.
  - Rollback safety: pre_state is captured before every mutation.
  - The no-op branch is mandatory, not optional.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import httpx

if TYPE_CHECKING:
    from app.clients.infoblox.session import InfobloxSession

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Return-field list for all GET calls — only confirmed fields from research.
# ---------------------------------------------------------------------------
_CNAME_RETURN_FIELDS = "name,canonical,view,ttl,use_ttl,_ref"

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

ActionTaken = Literal["created", "updated", "no_op", "deleted", "not_found"]


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class InfobloxError(Exception):
    """Base class for all Infoblox client errors."""


class InfobloxNotFoundError(InfobloxError):
    """Raised when a WAPI resource returns HTTP 404."""


class InfobloxConflictError(InfobloxError):
    """Raised when a duplicate-create returns HTTP 400 from Infoblox."""


class InfobloxTimeoutError(InfobloxError):
    """
    Raised when an httpx.TimeoutException occurs.

    Per CLAUDE.md: a timeout means the outcome is **unknown**.
    Never blind-retry.  The caller must read back to determine actual state
    and converge from there.
    """


class InfobloxServerError(InfobloxError):
    """Raised when Infoblox returns an HTTP 5xx response."""


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class OperationResult:
    """
    Outcome of a single idempotent CNAME operation.

    Attributes
    ----------
    action:
        One of ``"created"``, ``"updated"``, ``"no_op"``, ``"deleted"``,
        ``"not_found"``.
    pre_state:
        The CNAME record dict (including ``_ref``) as it existed **before**
        this call, or ``None`` if the object did not exist.
        Required for correct rollback: if ``pre_state`` is ``None`` the
        compensating step must delete; if it is a dict it must restore.
    post_state:
        The record dict after the operation, or ``None`` for deletes and
        not-found results.
    """

    action: ActionTaken
    pre_state: dict | None  # None  = object did not exist before this call
    post_state: dict | None  # None = object is gone / was never there


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class InfobloxClient:
    """
    High-level Infoblox WAPI operations for CNAME records.

    All methods follow the §3.3 step execution pattern:
      1. Read current state.
      2. Compare to desired state.
      3. Act only if different; no-op if identical.
      4. Return an OperationResult with pre_state captured before any write.

    This class does **not** handle retry logic.  Callers that need retry
    must check InfobloxTimeoutError and read back before re-issuing a write.
    """

    def __init__(self, session: InfobloxSession) -> None:
        self._session = session

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _raise_for_status(self, response: httpx.Response) -> None:
        """
        Map Infoblox HTTP error codes to typed exceptions.

        Parameters
        ----------
        response:
            An ``httpx.Response`` object.
        """
        if response.status_code == 404:
            raise InfobloxNotFoundError(
                f"Infoblox returned 404: {response.text}"
            )
        if response.status_code == 400:
            raise InfobloxConflictError(
                f"Infoblox returned 400 (likely duplicate or bad request): {response.text}"
            )
        if response.status_code >= 500:
            raise InfobloxServerError(
                f"Infoblox returned {response.status_code}: {response.text}"
            )
        if response.status_code >= 400:
            # Catch-all for unexpected 4xx codes not explicitly handled above.
            raise InfobloxError(
                f"Infoblox returned {response.status_code}: {response.text}"
            )

    # ------------------------------------------------------------------
    # CNAME operations
    # ------------------------------------------------------------------

    async def get_cname(
        self,
        name: str,
        view: str | None = None,
    ) -> dict | None:
        """
        Search for a CNAME record by name (and optional DNS view).

        Calls
        -----
        GET /wapi/v{ver}/record:cname
            ?name={name}
            [&view={view}]
            &_return_fields=name,canonical,view,ttl,use_ttl,_ref

        Returns
        -------
        dict
            The first matching record (including ``_ref``), or ``None`` if no
            record was found.

        Notes
        -----
        - Only field names confirmed in api-research-findings.md are requested.
        - If ``view`` is ``None`` the view param is omitted entirely.
        """
        params: dict[str, str] = {
            "name": name,
            "_return_fields": _CNAME_RETURN_FIELDS,
        }
        if view is not None:
            params["view"] = view

        response = await self._session.request(
            "GET",
            "record:cname",
            params=params,
        )
        self._raise_for_status(response)

        results: list[dict] = response.json()
        if not results:
            return None
        return results[0]

    async def ensure_cname(
        self,
        name: str,
        canonical: str,
        view: str | None = None,
        ttl: int | None = None,
        comment: str | None = None,
    ) -> OperationResult:
        """
        Idempotent create-or-update for a CNAME record.

        Algorithm (§3.3 of the implementation plan):
          1. GET current state → ``pre_state``.
          2. Not found → POST to create → ``action="created"``.
          3. Found and all supplied fields match → ``action="no_op"`` (no write).
          4. Found but differs → PUT ``{_ref}`` → ``action="updated"``.

        The no-op branch is not optional — it is the mechanism that makes
        every retry safe.

        Parameters
        ----------
        name:
            The alias FQDN (``record:cname`` ``name`` field).
        canonical:
            The target FQDN (``record:cname`` ``canonical`` field).
        view:
            DNS view.  If ``None``, omitted from create/update payloads and
            from the search query.
        ttl:
            TTL in seconds.  If supplied, ``use_ttl`` is set to ``True`` in
            the payload so Infoblox honours the value.  If ``None``, neither
            ``ttl`` nor ``use_ttl`` is included.
        comment:
            Optional free-text comment field.

        Returns
        -------
        OperationResult
        """
        # Step 1: read current state.
        pre_state = await self.get_cname(name, view=view)

        # Build the desired payload with only confirmed field names.
        body: dict = {
            "name": name,
            "canonical": canonical,
        }
        if view is not None:
            body["view"] = view
        if ttl is not None:
            body["ttl"] = ttl
            body["use_ttl"] = True
        if comment is not None:
            body["comment"] = comment

        # Step 2: not found → create.
        if pre_state is None:
            response = await self._session.request(
                "POST",
                "record:cname",
                json=body,
            )
            self._raise_for_status(response)
            # Infoblox returns the opaque _ref string on a successful POST.
            created_ref: str = response.json()
            # Read back to return a complete post_state dict.
            post_state = await self.get_cname(name, view=view)
            logger.info(
                "Infoblox CNAME created: name=%s canonical=%s ref=%s",
                name,
                canonical,
                created_ref,
            )
            return OperationResult(
                action="created",
                pre_state=None,
                post_state=post_state,
            )

        # Steps 3 & 4: record exists.  Compare desired vs actual.
        needs_update = False

        if pre_state.get("canonical") != canonical:
            needs_update = True
        if view is not None and pre_state.get("view") != view:
            needs_update = True
        if ttl is not None:
            if pre_state.get("ttl") != ttl or not pre_state.get("use_ttl"):
                needs_update = True

        # Step 3: identical → no-op.
        if not needs_update:
            logger.debug(
                "Infoblox CNAME no-op: name=%s is already correct.", name
            )
            return OperationResult(
                action="no_op",
                pre_state=pre_state,
                post_state=pre_state,
            )

        # Step 4: differs → update via PUT on the opaque _ref.
        ref: str = pre_state["_ref"]
        response = await self._session.request(
            "PUT",
            ref,
            json=body,
        )
        self._raise_for_status(response)
        post_state = await self.get_cname(name, view=view)
        logger.info(
            "Infoblox CNAME updated: name=%s canonical=%s ref=%s",
            name,
            canonical,
            ref,
        )
        return OperationResult(
            action="updated",
            pre_state=pre_state,
            post_state=post_state,
        )

    async def delete_cname(
        self,
        name: str,
        view: str | None = None,
    ) -> OperationResult:
        """
        Idempotent delete of a CNAME record.

        Algorithm:
          1. GET current state to obtain ``_ref`` and capture ``pre_state``
             for rollback.
          2. Not found → return ``action="not_found"`` (success; already gone).
          3. Found → DELETE ``{_ref}``.
             - HTTP 200/204 → success.
             - HTTP 404 → already gone between GET and DELETE; treat as success.

        Per CLAUDE.md rollback rule: ``pre_state`` is captured before every
        step.  A compensating action that needs to undo a delete will use
        ``pre_state`` to restore the record (because it did exist before this
        call).

        Parameters
        ----------
        name:
            The alias FQDN to delete.
        view:
            DNS view.  If ``None``, omitted from the search query.

        Returns
        -------
        OperationResult
        """
        # Step 1: read current state.
        pre_state = await self.get_cname(name, view=view)

        # Step 2: not found — already gone; this is success.
        if pre_state is None:
            logger.debug(
                "Infoblox CNAME delete no-op (not found): name=%s", name
            )
            return OperationResult(
                action="not_found",
                pre_state=None,
                post_state=None,
            )

        # Step 3: found — delete via the opaque _ref.
        ref: str = pre_state["_ref"]
        response = await self._session.request(
            "DELETE",
            ref,
        )

        if response.status_code == 404:
            # Deleted between our GET and this DELETE — treat as success.
            logger.debug(
                "Infoblox CNAME delete: 404 on DELETE (already removed): ref=%s",
                ref,
            )
        elif response.status_code not in (200, 204):
            self._raise_for_status(response)

        logger.info(
            "Infoblox CNAME deleted: name=%s ref=%s", name, ref
        )
        return OperationResult(
            action="deleted",
            pre_state=pre_state,
            post_state=None,
        )
