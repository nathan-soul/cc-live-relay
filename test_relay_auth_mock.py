#!/usr/bin/env python3
"""
Exercises the REQUIRE_WATCH_AUTH ticket flow and the ENABLE_SELF_VIEW_BLOCK IP check end-to-end
against a mocked GO Users/Me response — no real network call to GeneralsOnline, no real TCP
sockets at all (uses FastAPI's in-process ASGI TestClient for both HTTP and WebSocket). This is
deliberately independent of however the real GO-services mock/integration ends up working — it
only needs server.http_client to behave like an aiohttp.ClientSession, which is the one seam
server.py already calls through.

Self-view tests rely on the TestClient giving every connection (source and observer) the same
peer address ("testclient"), which is exactly the same-IP condition the block keys on.

Run: python test_relay_auth_mock.py
"""
import json
import struct
import sys
from contextlib import contextmanager

import server
from fastapi.testclient import TestClient

PASS = 0
FAIL = 0

MSG_REGISTER = 0
MSG_HEADER   = 1
MSG_BODY     = 3
MSG_END      = 4
MSG_ROLE     = 5
MSG_ERROR    = 6


def pack_frame(msg_type: int, payload: bytes = b"") -> bytes:
    return bytes([msg_type]) + struct.pack('<I', len(payload)) + payload


def unpack_frame(data: bytes):
    if len(data) < 5:
        return (None, b"")
    msg_type = data[0]
    payload_len = struct.unpack("<I", data[1:5])[0]
    if len(data) < 5 + payload_len:
        return (None, b"")
    return (msg_type, data[5:5 + payload_len])


def receive_until(ws, want_type: int, max_frames: int = 5) -> bytes:
    """Read frames until `want_type` is seen (catch-up always sends MSG_ROLE's config frame
    before MSG_HEADER, so admission checks can't assume the first frame received is the
    header). Raises if it isn't seen within max_frames."""
    for _ in range(max_frames):
        raw = ws.receive_bytes()
        msg_type, payload = unpack_frame(raw)
        if msg_type == want_type:
            return payload
        if msg_type == MSG_ERROR:
            raise AssertionError(f"got MSG_ERROR while waiting for type={want_type}: {payload!r}")
    raise AssertionError(f"type={want_type} not seen within {max_frames} frames")


def ok(name):
    global PASS
    PASS += 1
    print(f"  [PASS] {name}")


def fail(name, reason=""):
    global FAIL
    FAIL += 1
    print(f"  [FAIL] {name} -- {reason}")


# ═══════════════════════════════════════════════════════════════════════════
# Fake stand-in for aiohttp.ClientSession, mocking GO's Users/Me.
#
# Real shape confirmed live against api.playgenerals.online during design (401 with
# WWW-Authenticate: Bearer error="invalid_token" for a bad token) and from
# GenOnlineService/Controllers/User/UserController.cs's MyUser(): 200 {user_id, display_name}
# for a valid GameClient/ChatClient/GameLauncher-role token, 401 otherwise (the JWT bearer
# middleware rejects before the controller body runs, so there's no other status to model).
# ═══════════════════════════════════════════════════════════════════════════

class FakeUsersMeResponse:
    def __init__(self, status: int, body: dict):
        self.status = status
        self._body = body

    async def json(self):
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeHTTPClient:
    """Drop-in for the one aiohttp.ClientSession call site server.py uses (mint_ticket)."""

    def __init__(self):
        self._responses: dict[str, tuple[int, dict]] = {}

    def set_valid_token(self, token: str, user_id: int, display_name: str = "TestUser"):
        self._responses[token] = (200, {"user_id": user_id, "display_name": display_name})

    def set_token_status(self, token: str, status: int, body: dict = None):
        """Model GO's 403 (banned) / 404 (deleted user) / anything else — same shape as a real
        Users/Me response for that status, per the relay's per-status handling in mint_ticket()."""
        self._responses[token] = (status, body or {})

    async def close(self) -> None:
        pass  # nothing real to close; matches aiohttp.ClientSession's interface

    def get(self, url, headers=None, timeout=None):
        auth = (headers or {}).get("Authorization", "")
        token = auth[len("Bearer "):] if auth.startswith("Bearer ") else auth
        status, body = self._responses.get(token, (401, {}))
        return FakeUsersMeResponse(status, body)


# ═══════════════════════════════════════════════════════════════════════════
# Test helpers
# ═══════════════════════════════════════════════════════════════════════════

@contextmanager
def open_source(client: TestClient, lobby_id: str, player_name: str):
    """Keeps the source connection open for the duration of the `with` block.

    Ticket-minting and /watch checks need the session to still be alive (not `ended`) while
    they run — a source that disconnects right after sending END is exactly what legitimately
    ends a GameSession (see register_endpoint's finally -> remove_source), so the caller must
    do its ticket/watch work *inside* this block, not after it returns.
    """
    with client.websocket_connect("/register") as ws:
        ws.send_bytes(pack_frame(MSG_REGISTER, json.dumps({
            "lobbyid": lobby_id,
            "player_name": player_name,
            "can_stream": True,
            "is_host": True,
        }).encode()))
        raw = ws.receive_bytes()
        msg_type, payload = unpack_frame(raw)
        assert msg_type == MSG_ROLE, f"expected ROLE, got {msg_type}"
        ws.send_bytes(pack_frame(MSG_HEADER, f"HEADER_{lobby_id}".encode()))
        yield ws


# ═══════════════════════════════════════════════════════════════════════════
# Tests
# ═══════════════════════════════════════════════════════════════════════════

def test_ticket_mint_valid_token(client: TestClient, fake_http: FakeHTTPClient):
    print("\n=== Ticket mint: valid (mocked) token ===")
    with open_source(client, "auth_mock_001", "Host1"):
        fake_http.set_valid_token("good-token-1", user_id=42, display_name="Alice")
        r = client.get("/watch/auth_mock_001/ticket", headers={"Authorization": "Bearer good-token-1"})
        assert r.status_code == 200, f"expected 200, got {r.status_code}: {r.text}"
        data = r.json()
        assert "ticket=" in data["url"], f"ticket param missing from url: {data}"
        assert data["expires_in"] == server.WATCH_TICKET_TTL_SECONDS
        ok(f"mocked valid token -> 200, ticket URL minted ({data['url']})")


def test_ticket_mint_invalid_token(client: TestClient, fake_http: FakeHTTPClient):
    print("\n=== Ticket mint: invalid (mocked) token ===")
    with open_source(client, "auth_mock_002", "Host2"):
        r = client.get("/watch/auth_mock_002/ticket", headers={"Authorization": "Bearer garbage-token"})
        assert r.status_code == 401, f"expected 401, got {r.status_code}"
        ok("mocked invalid token -> 401, matching GO's real Users/Me behavior")


def test_ticket_mint_banned_user(client: TestClient, fake_http: FakeHTTPClient):
    print("\n=== Ticket mint: GO reports the account banned (403) ===")
    with open_source(client, "auth_mock_010", "Host10"):
        fake_http.set_token_status("banned-token", 403)
        r = client.get("/watch/auth_mock_010/ticket", headers={"Authorization": "Bearer banned-token"})
        assert r.status_code == 403, f"expected 403, got {r.status_code}"
        ok("GO 403 (banned/unauthorized) forwarded as-is, not collapsed into a generic 401")


def test_ticket_mint_deleted_user(client: TestClient, fake_http: FakeHTTPClient):
    print("\n=== Ticket mint: GO reports the account no longer exists (404) ===")
    with open_source(client, "auth_mock_011", "Host11"):
        fake_http.set_token_status("deleted-user-token", 404)
        r = client.get("/watch/auth_mock_011/ticket", headers={"Authorization": "Bearer deleted-user-token"})
        assert r.status_code == 404, f"expected 404, got {r.status_code}"
        ok("GO 404 (deleted user) forwarded as-is")


def test_ticket_mint_unexpected_status(client: TestClient, fake_http: FakeHTTPClient):
    print("\n=== Ticket mint: GO returns something unrecognized ===")
    with open_source(client, "auth_mock_012", "Host12"):
        fake_http.set_token_status("weird-token", 500)
        r = client.get("/watch/auth_mock_012/ticket", headers={"Authorization": "Bearer weird-token"})
        assert r.status_code == 502, f"expected 502 (unexpected upstream status), got {r.status_code}"
        ok("unrecognized Users/Me status -> 502, not silently treated as valid or invalid")


def test_ticket_mint_malformed_200(client: TestClient, fake_http: FakeHTTPClient):
    print("\n=== Ticket mint: malformed 200 bodies fail loudly, never mint silently ===")
    with open_source(client, "auth_mock_017", "Host17"):
        fake_http.set_token_status("list-body-token", 200, body=[{"user_id": 1}])
        r = client.get("/watch/auth_mock_017/ticket", headers={"Authorization": "Bearer list-body-token"})
        assert r.status_code == 502, f"expected 502 for non-object body, got {r.status_code}"
        ok("200 with a non-object body -> 502")

        fake_http.set_token_status("no-id-token", 200, body={"display_name": "Ghost"})
        r = client.get("/watch/auth_mock_017/ticket", headers={"Authorization": "Bearer no-id-token"})
        assert r.status_code == 502, f"expected 502 for missing user_id, got {r.status_code}"
        ok("200 missing user_id -> 502")

        fake_http.set_token_status("str-id-token", 200, body={"user_id": "not-an-int", "display_name": "X"})
        r = client.get("/watch/auth_mock_017/ticket", headers={"Authorization": "Bearer str-id-token"})
        assert r.status_code == 502, f"expected 502 for non-int user_id, got {r.status_code}"
        ok("200 with a string user_id -> 502")


def test_watch_requires_ticket_when_auth_on(client: TestClient, fake_http: FakeHTTPClient):
    print("\n=== /watch requires a ticket once REQUIRE_WATCH_AUTH is on ===")
    with open_source(client, "auth_mock_003", "Host3"):
        server.REQUIRE_WATCH_AUTH = True
        try:
            with client.websocket_connect("/watch/auth_mock_003") as ws:
                raw = ws.receive_bytes()
                msg_type, payload = unpack_frame(raw)
                assert msg_type == MSG_ERROR, f"expected ERROR with no ticket, got type={msg_type}"
                ok("no ticket param -> MSG_ERROR, connection rejected")
        finally:
            server.REQUIRE_WATCH_AUTH = False


def test_watch_admits_with_valid_ticket(client: TestClient, fake_http: FakeHTTPClient):
    print("\n=== /watch admits a connection carrying a valid ticket ===")
    with open_source(client, "auth_mock_004", "Host4"):
        fake_http.set_valid_token("good-token-4", user_id=99, display_name="Bob")
        r = client.get("/watch/auth_mock_004/ticket", headers={"Authorization": "Bearer good-token-4"})
        assert r.status_code == 200
        ticket_key = r.json()["url"].split("ticket=")[1]

        server.REQUIRE_WATCH_AUTH = True
        try:
            with client.websocket_connect(f"/watch/auth_mock_004?ticket={ticket_key}") as ws:
                receive_until(ws, MSG_HEADER)
                ok("valid ticket -> admitted, HEADER received")
        finally:
            server.REQUIRE_WATCH_AUTH = False


def test_ticket_is_single_use(client: TestClient, fake_http: FakeHTTPClient):
    print("\n=== A consumed ticket cannot be reused ===")
    with open_source(client, "auth_mock_005", "Host5"):
        fake_http.set_valid_token("good-token-5", user_id=7, display_name="Carol")
        r = client.get("/watch/auth_mock_005/ticket", headers={"Authorization": "Bearer good-token-5"})
        ticket_key = r.json()["url"].split("ticket=")[1]

        server.REQUIRE_WATCH_AUTH = True
        try:
            with client.websocket_connect(f"/watch/auth_mock_005?ticket={ticket_key}") as ws:
                receive_until(ws, MSG_HEADER)
            ok("first use of the ticket succeeds")

            with client.websocket_connect(f"/watch/auth_mock_005?ticket={ticket_key}") as ws:
                raw = ws.receive_bytes()
                msg_type, payload = unpack_frame(raw)
                assert msg_type == MSG_ERROR, f"expected ERROR on reuse, got type={msg_type}"
            ok("second use of the same ticket -> rejected (single-use enforced)")
        finally:
            server.REQUIRE_WATCH_AUTH = False


def test_ticket_wrong_lobby_rejected(client: TestClient, fake_http: FakeHTTPClient):
    print("\n=== A ticket minted for one lobby doesn't work on another ===")
    with open_source(client, "auth_mock_006", "Host6"), \
         open_source(client, "auth_mock_007", "Host7"):
        fake_http.set_valid_token("good-token-6", user_id=11, display_name="Dave")
        r = client.get("/watch/auth_mock_006/ticket", headers={"Authorization": "Bearer good-token-6"})
        ticket_key = r.json()["url"].split("ticket=")[1]

        server.REQUIRE_WATCH_AUTH = True
        try:
            with client.websocket_connect(f"/watch/auth_mock_007?ticket={ticket_key}") as ws:
                raw = ws.receive_bytes()
                msg_type, payload = unpack_frame(raw)
                assert msg_type == MSG_ERROR, f"expected ERROR for cross-lobby ticket, got type={msg_type}"
            ok("ticket minted for auth_mock_006 rejected on auth_mock_007")
        finally:
            server.REQUIRE_WATCH_AUTH = False


def test_watch_reconnect_also_requires_ticket(client: TestClient, fake_http: FakeHTTPClient):
    print("\n=== /watch-reconnect enforces the same ticket check ===")
    with open_source(client, "auth_mock_008", "Host8"):
        server.REQUIRE_WATCH_AUTH = True
        try:
            with client.websocket_connect("/watch-reconnect/auth_mock_008") as ws:
                raw = ws.receive_bytes()
                msg_type, payload = unpack_frame(raw)
                assert msg_type == MSG_ERROR, f"expected ERROR with no ticket, got type={msg_type}"
            ok("watch-reconnect with no ticket -> MSG_ERROR (not silently bypassed)")
        finally:
            server.REQUIRE_WATCH_AUTH = False


def test_watch_unaffected_when_auth_off(client: TestClient, fake_http: FakeHTTPClient):
    print("\n=== Regression: /watch works with no ticket when REQUIRE_WATCH_AUTH is off ===")
    with open_source(client, "auth_mock_009", "Host9"):
        assert server.REQUIRE_WATCH_AUTH is False, "test order bug: a prior test left auth ON"
        with client.websocket_connect("/watch/auth_mock_009") as ws:
            receive_until(ws, MSG_HEADER)
        ok("default (auth off) behavior unchanged")


def test_self_view_block_on_rejects(client: TestClient, fake_http: FakeHTTPClient):
    print("\n=== Self-view block: /watch from the streamer's own IP is rejected ===")
    with open_source(client, "auth_mock_013", "Host13"):
        server.ENABLE_SELF_VIEW_BLOCK = True
        try:
            with client.websocket_connect("/watch/auth_mock_013") as ws:
                raw = ws.receive_bytes()
                msg_type, payload = unpack_frame(raw)
                assert msg_type == MSG_ERROR, f"expected ERROR, got type={msg_type}"
                ok("observer with the source IP -> MSG_ERROR, connection closed")
        finally:
            server.ENABLE_SELF_VIEW_BLOCK = False


def test_self_view_block_on_rejects_reconnect(client: TestClient, fake_http: FakeHTTPClient):
    print("\n=== Self-view block: /watch-reconnect from the streamer's own IP is rejected ===")
    with open_source(client, "auth_mock_014", "Host14"):
        server.ENABLE_SELF_VIEW_BLOCK = True
        try:
            with client.websocket_connect("/watch-reconnect/auth_mock_014") as ws:
                raw = ws.receive_bytes()
                msg_type, payload = unpack_frame(raw)
                assert msg_type == MSG_ERROR, f"expected ERROR, got type={msg_type}"
                ok("reconnect with the source IP -> MSG_ERROR (no bypass via reconnect path)")
        finally:
            server.ENABLE_SELF_VIEW_BLOCK = False


def test_self_view_block_off_allows(client: TestClient, fake_http: FakeHTTPClient):
    print("\n=== Regression: /watch works from the source IP when ENABLE_SELF_VIEW_BLOCK is off ===")
    with open_source(client, "auth_mock_015", "Host15"):
        assert server.ENABLE_SELF_VIEW_BLOCK is False, "test order bug: a prior test left the block ON"
        with client.websocket_connect("/watch/auth_mock_015") as ws:
            receive_until(ws, MSG_HEADER)
        ok("default (self-view block off) behavior unchanged")


# ═══════════════════════════════════════════════════════════════════════════
# Runner
# ═══════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("RELAY WATCH-TICKET AUTH TESTS (mocked GO Users/Me, in-process, no real sockets)")
    print("=" * 60)

    with TestClient(server.app) as client:
        # Overwrite what the app's startup handler set (a real aiohttp.ClientSession) with our
        # fake — must happen after entering the context, since __enter__ runs the lifespan.
        # The real session is never used and would otherwise leak (and warn on exit), so it
        # gets closed here rather than just dropped.
        real_session = server.http_client
        fake_http = FakeHTTPClient()
        server.http_client = fake_http
        if real_session is not None:
            client.portal.call(real_session.close)

        tests = [
            test_ticket_mint_valid_token,
            test_ticket_mint_invalid_token,
            test_ticket_mint_banned_user,
            test_ticket_mint_deleted_user,
            test_ticket_mint_unexpected_status,
            test_ticket_mint_malformed_200,
            test_watch_requires_ticket_when_auth_on,
            test_watch_admits_with_valid_ticket,
            test_ticket_is_single_use,
            test_ticket_wrong_lobby_rejected,
            test_watch_reconnect_also_requires_ticket,
            test_watch_unaffected_when_auth_off,
            test_self_view_block_on_rejects,
            test_self_view_block_on_rejects_reconnect,
            test_self_view_block_off_allows,
        ]

        for test in tests:
            try:
                test(client, fake_http)
            except Exception as e:
                fail(test.__name__, str(e))
                server.REQUIRE_WATCH_AUTH = False  # don't let a failed test leak state
                server.ENABLE_SELF_VIEW_BLOCK = False

    print("\n" + "=" * 60)
    print(f"RESULTS: {PASS} passed, {FAIL} failed")
    print("=" * 60)
    return FAIL == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
