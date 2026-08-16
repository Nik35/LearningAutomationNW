"""
app/clients/infoblox/session.py
================================
Low-level HTTP session for Infoblox WAPI.

Authentication model (confirmed from api-research-findings.md):
  - First request carries  ``Authorization: Basic {b64(user:pass)}``.
  - The response sets     ``Set-Cookie: ibapauth=...``.
  - httpx's built-in cookie jar retains the cookie automatically.
  - Subsequent requests send ``Cookie: ibapauth=<value>``; no re-login needed.
  - On 401: clear the cookie jar, re-authenticate via Basic, retry once.
  - Session invalidation: ``POST /wapi/v{version}/logout``.

Hard rules (CLAUDE.md):
  - Timeouts raise ``InfobloxTimeoutError``; never blind-retry.
    The caller must read back to determine actual state.
  - WRITE operations must target the grid-master host (responsibility of
    the caller — this session accepts ``host`` at construction time and does
    not change it).
"""

from __future__ import annotations

import base64
import logging

import httpx

logger = logging.getLogger(__name__)


class InfobloxSession:
    """
    Manages a persistent httpx.AsyncClient for Infoblox WAPI calls.

    Cookie lifecycle
    ----------------
    httpx.AsyncClient is initialised with ``cookies`` support enabled via its
    default ``CookieJar``.  The first call that includes the ``Authorization:
    Basic`` header triggers Infoblox to set ``ibapauth`` in a ``Set-Cookie``
    response header.  httpx stores the cookie and sends it on all subsequent
    requests automatically — no manual cookie handling is required.

    Re-authentication
    -----------------
    If any response returns HTTP 401 the cookie jar is cleared and the request
    is retried once with ``Authorization: Basic``.  This covers session
    expiration without blind-looping.
    """

    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        wapi_version: str,
        verify_ssl: bool = True,
        timeout_seconds: float = 30.0,
    ) -> None:
        """
        Parameters
        ----------
        host:
            Hostname or IP of the Infoblox grid master (no scheme, no trailing
            slash).  All WRITE operations must target the grid master.
        username / password:
            WAPI credentials.  Used only for the initial Basic auth handshake
            and on session re-authentication after a 401.
        wapi_version:
            WAPI version string, e.g. ``"2.12"``.  Included in every path via
            :meth:`_wapi_path`.
        verify_ssl:
            Whether to verify the server's TLS certificate.  Should be ``True``
            in production.
        timeout_seconds:
            Total request timeout.  On expiry, ``InfobloxTimeoutError`` is
            raised — the caller must read back to determine actual state before
            retrying.
        """
        self.host = host
        self.username = username
        self.password = password
        self.wapi_version = wapi_version
        self._base_url = f"https://{host}"
        self._timeout = httpx.Timeout(timeout_seconds)

        # httpx.AsyncClient with keep-alive and a live cookie jar so that the
        # ibapauth cookie is retained across calls without any manual handling.
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            verify=verify_ssl,
            timeout=self._timeout,
            # follow_redirects keeps sessions working even if the grid master
            # issues a redirect on login.
            follow_redirects=True,
        )

        # True once we have a valid ibapauth cookie in the jar.
        self._authenticated = False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _wapi_path(self, resource: str) -> str:
        """Build the full WAPI path for a resource name or opaque _ref."""
        # resource may already be a full opaque ref like
        # "record:cname/ZG5z..." — do not double-prefix.
        if resource.startswith("/wapi/"):
            return resource
        return f"/wapi/v{self.wapi_version}/{resource}"

    def _basic_auth_header(self) -> str:
        """Return the value for the ``Authorization`` header."""
        token = base64.b64encode(
            f"{self.username}:{self.password}".encode()
        ).decode()
        return f"Basic {token}"

    def _clear_auth(self) -> None:
        """Remove all cookies from the jar (forces re-authentication)."""
        self._client.cookies.clear()
        self._authenticated = False

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def request(
        self,
        method: str,
        path: str,
        **kwargs: object,
    ) -> httpx.Response:
        """
        Execute an authenticated WAPI request.

        On first call (or after session expiry / explicit clear):
          - Attaches ``Authorization: Basic`` so Infoblox will set the
            ``ibapauth`` cookie in its response.

        On subsequent calls:
          - The cookie jar already contains ``ibapauth``; httpx sends it
            automatically without any manual intervention.

        On 401:
          - Clears the cookie jar and retries once with Basic auth.

        On timeout:
          - Raises :exc:`InfobloxTimeoutError` immediately.
            **Caller is responsible for reading back state before retrying.**

        Parameters
        ----------
        method:
            HTTP method string (``"GET"``, ``"POST"``, ``"PUT"``, ``"DELETE"``).
        path:
            Relative path under ``/wapi/v{version}/`` **or** the full path
            starting with ``/wapi/``.  Use :meth:`_wapi_path` to build it.
        **kwargs:
            Passed directly to ``httpx.AsyncClient.request``.
        """
        from app.clients.infoblox.records import InfobloxTimeoutError  # local import avoids circular

        full_path = self._wapi_path(path)

        headers: dict[str, str] = dict(kwargs.pop("headers", {}) or {})

        # Attach Basic auth if we have not yet authenticated (or after logout /
        # 401-triggered clear).
        if not self._authenticated:
            headers["Authorization"] = self._basic_auth_header()

        try:
            response = await self._client.request(
                method,
                full_path,
                headers=headers,
                **kwargs,
            )
        except httpx.TimeoutException as exc:
            raise InfobloxTimeoutError(
                f"Request timed out: {method} {full_path}"
            ) from exc

        # Mark authenticated once we have received any successful response —
        # the ibapauth cookie will now be in the jar.
        if response.status_code < 400:
            self._authenticated = True
            return response

        if response.status_code == 401:
            # Session expired or credentials wrong.  Clear the jar and retry
            # once with explicit Basic auth.
            logger.warning(
                "Infoblox returned 401; clearing session cookie and retrying once."
            )
            self._clear_auth()
            retry_headers: dict[str, str] = dict(kwargs.pop("headers", {}) or {})
            retry_headers["Authorization"] = self._basic_auth_header()
            try:
                response = await self._client.request(
                    method,
                    full_path,
                    headers=retry_headers,
                    **kwargs,
                )
            except httpx.TimeoutException as exc:
                raise InfobloxTimeoutError(
                    f"Request timed out on re-auth retry: {method} {full_path}"
                ) from exc
            if response.status_code < 400:
                self._authenticated = True

        return response

    async def logout(self) -> None:
        """
        Invalidate the current WAPI session on the grid master.

        ``POST /wapi/v{version}/logout``

        Errors during logout are logged but not re-raised — the caller is
        shutting down and there is nothing useful to do with a logout failure.
        After this call the cookie jar is cleared regardless of the HTTP
        response so the next :meth:`request` will re-authenticate.
        """
        try:
            response = await self.request("POST", "logout")
            if response.status_code not in (200, 204):
                logger.warning(
                    "Infoblox logout returned unexpected status %d",
                    response.status_code,
                )
        except Exception:  # noqa: BLE001
            logger.exception("Exception during Infoblox logout (ignored)")
        finally:
            self._clear_auth()

    async def aclose(self) -> None:
        """Close the underlying httpx client and release connections."""
        await self._client.aclose()
