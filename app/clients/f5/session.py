"""
app/clients/f5/session.py
=========================
One persistent ``httpx.AsyncClient`` per F5 device.

Design constraints (from CLAUDE.md and the implementation plan):
- Connection pool size comes from ``max_connections`` (P-1 derived).  Never
  hardcoded here.
- ``timeout_seconds`` is a constructor parameter.  F5 calls are slow; callers
  supply the value from config.  Never hardcoded.
- TLS verification is configurable so that dev environments with self-signed
  certs work without patching source code.
- Keep-alive is httpx's default behaviour for an AsyncClient; we do not
  disable it.
"""

from __future__ import annotations

import httpx


class F5Session:
    """
    Thin wrapper around ``httpx.AsyncClient`` scoped to a single F5 device.

    Parameters
    ----------
    device_id:
        Logical identifier for the device (used for logging / Redis key
        namespacing).  Not sent on the wire.
    host:
        Hostname or IP of the BIG-IP management interface.
    max_connections:
        Maximum number of simultaneous TCP connections in the pool.
        Comes from P-1 (per-device concurrency) — never hardcode.
    timeout_seconds:
        Total request timeout in seconds.  F5 config writes are slow;
        callers must supply a value from application config.
        Never hardcode.
    verify_ssl:
        Whether to verify the server's TLS certificate.  Default ``True``
        (production).  Set ``False`` only in dev/test environments.
    """

    def __init__(
        self,
        device_id: str,
        host: str,
        max_connections: int,
        timeout_seconds: float,
        verify_ssl: bool = True,
    ) -> None:
        self.device_id = device_id
        self._client = httpx.AsyncClient(
            base_url=f"https://{host}/",
            verify=verify_ssl,
            timeout=httpx.Timeout(timeout_seconds),
            limits=httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_connections,
            ),
        )

    async def request(self, method: str, path: str, **kwargs: object) -> httpx.Response:
        """
        Dispatch an HTTP request through the pooled client.

        ``path`` must start with ``/`` (e.g. ``/mgmt/tm/gtm/wideip/a``).
        Additional keyword arguments are forwarded verbatim to
        ``httpx.AsyncClient.request``.

        Raises
        ------
        httpx.TimeoutException
            Propagated unchanged; callers in ``gtm.py`` convert it to
            ``F5TimeoutError``.  The session layer does NOT retry — a
            timeout means the outcome is unknown (see §3.3 rule 7 in the
            implementation plan).
        """
        return await self._client.request(method, path, **kwargs)  # type: ignore[arg-type]

    async def aclose(self) -> None:
        """Release all connections.  Call on application shutdown."""
        await self._client.aclose()
