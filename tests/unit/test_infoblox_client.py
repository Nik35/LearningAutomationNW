"""
tests/unit/test_infoblox_client.py
===================================
Unit tests for the Infoblox WAPI client (session + records).

Uses ``respx`` to mock httpx so no real network calls are made.

Coverage
--------
TestGetCname
    - Returns None when Infoblox returns an empty list.
    - Returns the first dict (with _ref) when a match is found.
    - Omits the view query param when view=None.
    - Includes the view query param when view is supplied.

TestEnsureCname
    - "created" when the record is absent (POSTs the correct body).
    - "no_op" when canonical, view, and ttl all match (no PUT issued).
    - "no_op" when canonical matches and ttl/view are not supplied.
    - "updated" when canonical differs (PUTs to the existing _ref).
    - "updated" when ttl differs (PUTs to the existing _ref).
    - OperationResult.pre_state is None on "created".
    - OperationResult.pre_state contains the old record on "updated".

TestDeleteCname
    - "deleted" on success (200 from DELETE).
    - "deleted" on success (204 from DELETE).
    - "deleted" when DELETE itself returns 404 (race condition; still success).
    - "not_found" when GET returns empty list (no DELETE issued).

TestSessionReauth
    - 401 from Infoblox clears cookie and retries with Basic auth once.
    - Retry after 401 sets _authenticated if the second call succeeds.

TestSessionTimeout
    - httpx.TimeoutException is translated to InfobloxTimeoutError.
    - InfobloxTimeoutError is never silently swallowed.

TestCookieReuse
    - The ibapauth cookie set in the first response is sent in subsequent calls.
    - Basic auth header is NOT included in subsequent calls.

TestErrorMapping
    - 404 from a GET/DELETE raises InfobloxNotFoundError.
    - 400 raises InfobloxConflictError.
    - 5xx raises InfobloxServerError.
"""

from __future__ import annotations

import base64
import json

import httpx
import pytest
import respx

from app.clients.infoblox.records import (
    InfobloxClient,
    InfobloxConflictError,
    InfobloxNotFoundError,
    InfobloxServerError,
    InfobloxTimeoutError,
)
from app.clients.infoblox.session import InfobloxSession

# ---------------------------------------------------------------------------
# Constants used across tests
# ---------------------------------------------------------------------------

HOST = "infoblox.example.com"
USER = "admin"
PASS = "secret"
VERSION = "2.12"
BASE = f"https://{HOST}"

CNAME_REF = "record:cname/ZG5zLm5ldHdvcmtfdmlldyQw:alias.example.com/default"
# Full URL path that httpx will request when using the opaque ref.
CNAME_REF_URL = f"https://{HOST}/wapi/v{VERSION}/{CNAME_REF}"
CNAME_RECORD = {
    "_ref": CNAME_REF,
    "name": "alias.example.com",
    "canonical": "target.example.com",
    "view": "default",
    "ttl": 300,
    "use_ttl": True,
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_session(**kwargs: object) -> InfobloxSession:
    """Return an InfobloxSession pointed at HOST with test credentials."""
    return InfobloxSession(
        host=HOST,
        username=USER,
        password=PASS,
        wapi_version=VERSION,
        verify_ssl=False,
        timeout_seconds=5.0,
        **kwargs,
    )


def _make_client(session: InfobloxSession | None = None) -> InfobloxClient:
    if session is None:
        session = _make_session()
    return InfobloxClient(session)


def _basic_header() -> str:
    token = base64.b64encode(f"{USER}:{PASS}".encode()).decode()
    return f"Basic {token}"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def session() -> InfobloxSession:
    return _make_session()


@pytest.fixture()
def client(session: InfobloxSession) -> InfobloxClient:
    return InfobloxClient(session)


# ===========================================================================
# TestGetCname
# ===========================================================================


class TestGetCname:
    """Tests for InfobloxClient.get_cname."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_none_on_empty_results(self, client: InfobloxClient) -> None:
        respx.get(f"{BASE}/wapi/v{VERSION}/record:cname").mock(
            return_value=httpx.Response(200, json=[])
        )
        result = await client.get_cname("alias.example.com")
        assert result is None

    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_dict_with_ref_on_match(self, client: InfobloxClient) -> None:
        respx.get(f"{BASE}/wapi/v{VERSION}/record:cname").mock(
            return_value=httpx.Response(200, json=[CNAME_RECORD])
        )
        result = await client.get_cname("alias.example.com")
        assert result is not None
        assert result["_ref"] == CNAME_REF
        assert result["name"] == "alias.example.com"
        assert result["canonical"] == "target.example.com"

    @pytest.mark.asyncio
    @respx.mock
    async def test_omits_view_param_when_none(self, client: InfobloxClient) -> None:
        route = respx.get(f"{BASE}/wapi/v{VERSION}/record:cname").mock(
            return_value=httpx.Response(200, json=[])
        )
        await client.get_cname("alias.example.com", view=None)
        # The request must NOT contain a view param.
        sent_request = route.calls[0].request
        assert "view" not in str(sent_request.url.params)

    @pytest.mark.asyncio
    @respx.mock
    async def test_includes_view_param_when_supplied(self, client: InfobloxClient) -> None:
        route = respx.get(f"{BASE}/wapi/v{VERSION}/record:cname").mock(
            return_value=httpx.Response(200, json=[CNAME_RECORD])
        )
        await client.get_cname("alias.example.com", view="internal")
        sent_request = route.calls[0].request
        assert "view=internal" in str(sent_request.url.params)


# ===========================================================================
# TestEnsureCname
# ===========================================================================


class TestEnsureCname:
    """Tests for InfobloxClient.ensure_cname."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_created_when_absent(self, client: InfobloxClient) -> None:
        """POST is called and action='created' when the record does not exist."""
        # GET → empty list
        respx.get(f"{BASE}/wapi/v{VERSION}/record:cname").mock(
            side_effect=[
                httpx.Response(200, json=[]),          # first call: pre-state check
                httpx.Response(200, json=[CNAME_RECORD]),  # second call: read-back after create
            ]
        )
        post_route = respx.post(f"{BASE}/wapi/v{VERSION}/record:cname").mock(
            return_value=httpx.Response(201, json=CNAME_REF)
        )

        result = await client.ensure_cname(
            name="alias.example.com",
            canonical="target.example.com",
            view="default",
        )

        assert result.action == "created"
        assert result.pre_state is None
        assert result.post_state is not None
        assert result.post_state["_ref"] == CNAME_REF
        assert post_route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_op_when_identical(self, client: InfobloxClient) -> None:
        """No PUT is issued when canonical, view, and ttl all match."""
        respx.get(f"{BASE}/wapi/v{VERSION}/record:cname").mock(
            return_value=httpx.Response(200, json=[CNAME_RECORD])
        )
        put_route = respx.put(url__startswith=BASE).mock(
            return_value=httpx.Response(200, json=CNAME_RECORD)
        )

        result = await client.ensure_cname(
            name="alias.example.com",
            canonical="target.example.com",
            view="default",
            ttl=300,
        )

        assert result.action == "no_op"
        assert result.pre_state == CNAME_RECORD
        assert result.post_state == CNAME_RECORD
        assert not put_route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_op_when_canonical_matches_and_no_ttl_view(
        self, client: InfobloxClient
    ) -> None:
        """
        When ttl and view are not supplied by the caller, only canonical is
        compared.  A record that matches on canonical alone is a no-op.
        """
        record_without_ttl = {
            "_ref": CNAME_REF,
            "name": "alias.example.com",
            "canonical": "target.example.com",
            "view": "default",
            "ttl": 0,
            "use_ttl": False,
        }
        respx.get(f"{BASE}/wapi/v{VERSION}/record:cname").mock(
            return_value=httpx.Response(200, json=[record_without_ttl])
        )
        put_route = respx.put(url__startswith=BASE).mock(
            return_value=httpx.Response(200, json=record_without_ttl)
        )

        result = await client.ensure_cname(
            name="alias.example.com",
            canonical="target.example.com",
            # view and ttl intentionally omitted
        )

        assert result.action == "no_op"
        assert not put_route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_updated_when_canonical_differs(self, client: InfobloxClient) -> None:
        """PUT is called on the existing _ref when canonical has changed."""
        # After the update, Infoblox returns the record with new canonical.
        updated_record = {**CNAME_RECORD, "canonical": "new-target.example.com"}

        respx.get(f"{BASE}/wapi/v{VERSION}/record:cname").mock(
            side_effect=[
                httpx.Response(200, json=[CNAME_RECORD]),      # pre-state
                httpx.Response(200, json=[updated_record]),    # read-back
            ]
        )
        put_route = respx.put(CNAME_REF_URL).mock(
            return_value=httpx.Response(200, json=CNAME_REF)
        )

        result = await client.ensure_cname(
            name="alias.example.com",
            canonical="new-target.example.com",
            view="default",
        )

        assert result.action == "updated"
        assert result.pre_state == CNAME_RECORD
        assert result.post_state == updated_record
        assert put_route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_updated_when_ttl_differs(self, client: InfobloxClient) -> None:
        """PUT is called when the TTL stored differs from the desired TTL."""
        updated_record = {**CNAME_RECORD, "ttl": 600}

        respx.get(f"{BASE}/wapi/v{VERSION}/record:cname").mock(
            side_effect=[
                httpx.Response(200, json=[CNAME_RECORD]),      # pre-state (ttl=300)
                httpx.Response(200, json=[updated_record]),    # read-back (ttl=600)
            ]
        )
        put_route = respx.put(CNAME_REF_URL).mock(
            return_value=httpx.Response(200, json=CNAME_REF)
        )

        result = await client.ensure_cname(
            name="alias.example.com",
            canonical="target.example.com",
            view="default",
            ttl=600,
        )

        assert result.action == "updated"
        assert put_route.called
        # Verify the PUT body includes use_ttl=True
        put_body = put_route.calls[0].request
        sent_body = json.loads(put_body.content)
        assert sent_body.get("use_ttl") is True
        assert sent_body.get("ttl") == 600


# ===========================================================================
# TestDeleteCname
# ===========================================================================


class TestDeleteCname:
    """Tests for InfobloxClient.delete_cname."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_deleted_on_200(self, client: InfobloxClient) -> None:
        respx.get(f"{BASE}/wapi/v{VERSION}/record:cname").mock(
            return_value=httpx.Response(200, json=[CNAME_RECORD])
        )
        respx.delete(CNAME_REF_URL).mock(
            return_value=httpx.Response(200, json=CNAME_REF)
        )

        result = await client.delete_cname("alias.example.com")

        assert result.action == "deleted"
        assert result.pre_state == CNAME_RECORD
        assert result.post_state is None

    @pytest.mark.asyncio
    @respx.mock
    async def test_deleted_on_204(self, client: InfobloxClient) -> None:
        respx.get(f"{BASE}/wapi/v{VERSION}/record:cname").mock(
            return_value=httpx.Response(200, json=[CNAME_RECORD])
        )
        respx.delete(CNAME_REF_URL).mock(
            return_value=httpx.Response(204)
        )

        result = await client.delete_cname("alias.example.com")

        assert result.action == "deleted"
        assert result.pre_state == CNAME_RECORD

    @pytest.mark.asyncio
    @respx.mock
    async def test_deleted_when_delete_returns_404_race(
        self, client: InfobloxClient
    ) -> None:
        """
        Record exists at GET time but 404 on DELETE (deleted between the two
        calls).  This is treated as success — the record is gone.
        """
        respx.get(f"{BASE}/wapi/v{VERSION}/record:cname").mock(
            return_value=httpx.Response(200, json=[CNAME_RECORD])
        )
        respx.delete(CNAME_REF_URL).mock(
            return_value=httpx.Response(404)
        )

        result = await client.delete_cname("alias.example.com")

        assert result.action == "deleted"
        assert result.pre_state == CNAME_RECORD
        assert result.post_state is None

    @pytest.mark.asyncio
    @respx.mock
    async def test_not_found_when_get_returns_empty(
        self, client: InfobloxClient
    ) -> None:
        """No DELETE is issued when the record is already gone."""
        respx.get(f"{BASE}/wapi/v{VERSION}/record:cname").mock(
            return_value=httpx.Response(200, json=[])
        )
        delete_route = respx.delete(url__startswith=BASE).mock(
            return_value=httpx.Response(200)
        )

        result = await client.delete_cname("alias.example.com")

        assert result.action == "not_found"
        assert result.pre_state is None
        assert result.post_state is None
        assert not delete_route.called


# ===========================================================================
# TestSessionReauth
# ===========================================================================


class TestSessionReauth:
    """Tests for 401-triggered re-authentication in InfobloxSession."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_reauth_on_401_retries_once(self, session: InfobloxSession) -> None:
        """
        When Infoblox returns 401, the session clears its cookie and retries
        the same request once with Basic auth included.
        """
        route = respx.get(f"{BASE}/wapi/v{VERSION}/record:cname").mock(
            side_effect=[
                # First call: authenticated state, Infoblox responds 401.
                httpx.Response(401, json={"text": "AdmConSessionExpiredError"}),
                # Second call: re-auth attempt succeeds.
                httpx.Response(200, json=[CNAME_RECORD]),
            ]
        )
        # Pre-warm so the first real call does NOT trigger Basic auth (simulates
        # a session that was previously authenticated).
        session._authenticated = True

        response = await session.request(
            "GET",
            session._wapi_path("record:cname"),
            params={"name": "alias.example.com", "_return_fields": "name,_ref"},
        )

        assert response.status_code == 200
        assert route.call_count == 2

        # The second request must include Authorization: Basic
        second_request = route.calls[1].request
        assert "Authorization" in second_request.headers
        assert second_request.headers["Authorization"].startswith("Basic ")

    @pytest.mark.asyncio
    @respx.mock
    async def test_authenticated_flag_set_after_successful_reauth(
        self, session: InfobloxSession
    ) -> None:
        """After a successful 401-retry the session is marked authenticated."""
        respx.get(f"{BASE}/wapi/v{VERSION}/record:cname").mock(
            side_effect=[
                httpx.Response(401),
                httpx.Response(200, json=[]),
            ]
        )
        session._authenticated = True

        await session.request(
            "GET",
            session._wapi_path("record:cname"),
            params={"name": "x", "_return_fields": "name"},
        )

        assert session._authenticated is True


# ===========================================================================
# TestSessionTimeout
# ===========================================================================


class TestSessionTimeout:
    """Tests that httpx.TimeoutException maps to InfobloxTimeoutError."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_timeout_raises_infoblox_timeout_error(
        self, session: InfobloxSession
    ) -> None:
        respx.get(f"{BASE}/wapi/v{VERSION}/record:cname").mock(
            side_effect=httpx.TimeoutException("timed out")
        )

        with pytest.raises(InfobloxTimeoutError):
            await session.request(
                "GET",
                session._wapi_path("record:cname"),
                params={"name": "x", "_return_fields": "name"},
            )

    @pytest.mark.asyncio
    @respx.mock
    async def test_timeout_on_reauth_retry_also_raises(
        self, session: InfobloxSession
    ) -> None:
        """A timeout on the 401-retry path also surfaces as InfobloxTimeoutError."""
        respx.get(f"{BASE}/wapi/v{VERSION}/record:cname").mock(
            side_effect=[
                httpx.Response(401),
                httpx.TimeoutException("timed out on retry"),
            ]
        )
        session._authenticated = True

        with pytest.raises(InfobloxTimeoutError):
            await session.request(
                "GET",
                session._wapi_path("record:cname"),
                params={"name": "x", "_return_fields": "name"},
            )


# ===========================================================================
# TestCookieReuse
# ===========================================================================


class TestCookieReuse:
    """
    Verify that ibapauth is reused across calls and Basic auth is not sent
    on subsequent requests.

    respx does not fully emulate httpx's cookie jar (it bypasses it), so we
    test this by directly inspecting InfobloxSession._authenticated and by
    checking that the ``Authorization`` header is present on the first call
    and absent on subsequent calls when _authenticated is True.
    """

    @pytest.mark.asyncio
    @respx.mock
    async def test_first_call_includes_basic_auth(
        self, session: InfobloxSession
    ) -> None:
        route = respx.get(f"{BASE}/wapi/v{VERSION}/record:cname").mock(
            return_value=httpx.Response(
                200,
                json=[],
                headers={"Set-Cookie": "ibapauth=abc123; Path=/"},
            )
        )

        # _authenticated starts False → Basic auth must be sent.
        assert session._authenticated is False
        await session.request(
            "GET",
            session._wapi_path("record:cname"),
            params={"name": "x", "_return_fields": "name"},
        )

        first_request = route.calls[0].request
        assert "Authorization" in first_request.headers
        assert first_request.headers["Authorization"].startswith("Basic ")

    @pytest.mark.asyncio
    @respx.mock
    async def test_subsequent_calls_skip_basic_auth(
        self, session: InfobloxSession
    ) -> None:
        route = respx.get(f"{BASE}/wapi/v{VERSION}/record:cname").mock(
            return_value=httpx.Response(200, json=[])
        )

        # Mark as already authenticated (cookie already in jar from prior call).
        session._authenticated = True

        await session.request(
            "GET",
            session._wapi_path("record:cname"),
            params={"name": "x", "_return_fields": "name"},
        )

        request = route.calls[0].request
        assert "Authorization" not in request.headers

    @pytest.mark.asyncio
    @respx.mock
    async def test_authenticated_flag_set_after_first_successful_response(
        self, session: InfobloxSession
    ) -> None:
        respx.get(f"{BASE}/wapi/v{VERSION}/record:cname").mock(
            return_value=httpx.Response(
                200,
                json=[],
                headers={"Set-Cookie": "ibapauth=abc123; Path=/"},
            )
        )

        assert session._authenticated is False
        await session.request(
            "GET",
            session._wapi_path("record:cname"),
            params={"name": "x", "_return_fields": "name"},
        )
        assert session._authenticated is True


# ===========================================================================
# TestErrorMapping
# ===========================================================================


class TestErrorMapping:
    """Tests that HTTP error codes map to the correct exception types."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_404_raises_not_found(self, client: InfobloxClient) -> None:
        respx.get(f"{BASE}/wapi/v{VERSION}/record:cname").mock(
            return_value=httpx.Response(404, json={"text": "Object not found"})
        )
        with pytest.raises(InfobloxNotFoundError):
            await client.get_cname("missing.example.com")

    @pytest.mark.asyncio
    @respx.mock
    async def test_400_raises_conflict(self, client: InfobloxClient) -> None:
        """
        A 400 during ensure_cname (POST) raises InfobloxConflictError.
        This tests the error-mapping in _raise_for_status.
        """
        # GET returns empty (record absent) so POST is attempted.
        respx.get(f"{BASE}/wapi/v{VERSION}/record:cname").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.post(f"{BASE}/wapi/v{VERSION}/record:cname").mock(
            return_value=httpx.Response(
                400,
                json={"text": "IbapDuplicateObjectError: Object already exists"},
            )
        )
        with pytest.raises(InfobloxConflictError):
            await client.ensure_cname(
                name="alias.example.com",
                canonical="target.example.com",
            )

    @pytest.mark.asyncio
    @respx.mock
    async def test_5xx_raises_server_error(self, client: InfobloxClient) -> None:
        respx.get(f"{BASE}/wapi/v{VERSION}/record:cname").mock(
            return_value=httpx.Response(503, text="Service Unavailable")
        )
        with pytest.raises(InfobloxServerError):
            await client.get_cname("alias.example.com")
