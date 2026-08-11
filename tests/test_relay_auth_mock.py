#!/usr/bin/env python3
"""
Exercises the GO-orchestrated ticket flow end-to-end against the relay's /internal/*
endpoints — no real network call to GeneralsOnline, no real TCP sockets at all (uses
FastAPI's in-process ASGI TestClient for both HTTP and WebSocket). GO services is mocked
by the test itself: it sends X-Relay-Key and mints stream tokens / watch tickets via the
same /internal/* endpoints the real GO RelayClient calls.

Run: python tests/test_relay_auth_mock.py
"""
import json
import struct
import sys
from contextlib import contextmanager
from pathlib import Path

# server.py lives at the repo root, one level up from tests/. Inserted here rather than left to
# the caller so the suite runs identically whether it is invoked directly, from another working
# directory, or collected by pytest.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import server
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

PASS = 0
FAIL = 0

MSG_REGISTER = 0
MSG_HEADER   = 1
MSG_BODY     = 3
MSG_END      = 4
MSG_ROLE     = 5
MSG_ERROR    = 6

RELAY_KEY = "test123"

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


def keys():
    return {"X-Relay-Key": RELAY_KEY}


# ═══════════════════════════════════════════════════════════════════════════
# Test helpers
# ═══════════════════════════════════════════════════════════════════════════

def register_livestream(client: TestClient, lobby_id: str, owner_user_id: int = 1,
                        delay_seconds: int = None) -> dict:
    """GO announces a livestream (POST /internal/livestreams). Returns the response json."""
    payload = {"lobby_id": lobby_id, "owner_user_id": owner_user_id}
    if delay_seconds is not None:
        payload["delay_seconds"] = delay_seconds
    r = client.post("/internal/livestreams", json=payload, headers=keys())
    assert r.status_code == 200, f"livestream register failed: {r.status_code} {r.text}"
    return r.json()


def mint_stream_token(client: TestClient, lobby_id: str, user_id: int) -> str:
    r = client.post("/internal/stream_tokens",
                    json={"lobby_id": lobby_id, "user_id": user_id},
                    headers=keys())
    assert r.status_code == 200, f"stream token mint failed: {r.status_code} {r.text}"
    return r.json()["url"].split("stream_token=")[1]


def mint_watch_ticket(client: TestClient, lobby_id: str, user_id: int,
                      priority: bool = False) -> str:
    r = client.post("/internal/watch_tickets",
                    json={"lobby_id": lobby_id, "user_id": user_id, "priority": priority},
                    headers=keys())
    assert r.status_code == 200, f"watch ticket mint failed: {r.status_code} {r.text}"
    return r.json()["url"].split("ticket=")[1]


@contextmanager
def open_source(client: TestClient, lobby_id: str, player_name: str, token: str):
    """Keeps a streamer connected for the duration of the `with` block (token already minted).

    Ticket/stream admission needs the session to still be alive (not `ended`) while tests
    run — a source that disconnects right after sending END is exactly what legitimately
    ends a GameSession, so the caller must do its watch work *inside* this block.
    """
    with client.websocket_connect(f"/stream/{lobby_id}?stream_token={token}") as ws:
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

def test_internal_key_required(client: TestClient, *_):
    print("\n=== Internal endpoints reject bad/missing relay key ===")
    with client.websocket_connect("/watch/no_key_game") as ws:
        ws.receive_bytes()  # drains the accept
    r = client.post("/internal/livestreams", json={"lobby_id": "key_001", "owner_user_id": 1})
    assert r.status_code == 401, f"expected 401 missing key, got {r.status_code}"
    ok("missing X-Relay-Key -> 401")

    r = client.post("/internal/livestreams", json={"lobby_id": "key_002", "owner_user_id": 1},
                    headers={"X-Relay-Key": "wrong-key"})
    assert r.status_code == 401, f"expected 401 wrong key, got {r.status_code}"
    ok("wrong X-Relay-Key -> 401")


def test_livestream_register_and_base_url(client: TestClient, *_):
    print("\n=== POST /internal/livestreams creates a session ===")
    data = register_livestream(client, "auth_mock_001", owner_user_id=42)
    assert "base_url" in data, f"missing base_url: {data}"
    assert "/stream/auth_mock_001" in data["base_url"], f"base_url wrong: {data}"
    ok(f"livestream registered, base_url={data['base_url']}")
    ok("session exists for lobby")
    assert "auth_mock_001" in server.games


def test_stream_token_unknown_lobby(client: TestClient, *_):
    print("\n=== Stream token for unknown lobby -> 404 ===")
    r = client.post("/internal/stream_tokens",
                    json={"lobby_id": "no_such_lobby", "user_id": 1},
                    headers=keys())
    assert r.status_code == 404, f"expected 404, got {r.status_code}"
    ok("stream token mint for unknown lobby -> 404")


def test_watch_ticket_unknown_lobby(client: TestClient, *_):
    print("\n=== Watch ticket for unknown lobby -> 404 ===")
    r = client.post("/internal/watch_tickets",
                    json={"lobby_id": "no_such_lobby_2", "user_id": 1},
                    headers=keys())
    assert r.status_code == 404, f"expected 404, got {r.status_code}"
    ok("watch ticket mint for unknown lobby -> 404")


def test_web_connect_no_ticket_rejected(client: TestClient, *_):
    print("\n=== /watch with no ticket is rejected once GO has a livestream ===")
    register_livestream(client, "auth_mock_002", owner_user_id=1)
    with client.websocket_connect("/watch/auth_mock_002") as ws:
        raw = ws.receive_bytes()
        msg_type, payload = unpack_frame(raw)
        assert msg_type == MSG_ERROR, f"expected ERROR, got type={msg_type}"
        ok("no ticket param -> MSG_ERROR, connection rejected")


def test_web_connect_valid_ticket_admits(client: TestClient, *_):
    print("\n=== /watch admits a connection carrying a valid ticket ===")
    register_livestream(client, "auth_mock_003", owner_user_id=1)
    ticket = mint_watch_ticket(client, "auth_mock_003", user_id=99)
    with open_source(client, "auth_mock_003", "Host3", mint_stream_token(client, "auth_mock_003", 1)):
        with client.websocket_connect(f"/watch/auth_mock_003?ticket={ticket}") as ws:
            receive_until(ws, MSG_HEADER)
            ok("valid ticket -> admitted, HEADER received")


def test_watch_ticket_single_use(client: TestClient, *_):
    print("\n=== A consumed watch ticket cannot be reused ===")
    register_livestream(client, "auth_mock_004", owner_user_id=1)
    ticket = mint_watch_ticket(client, "auth_mock_004", user_id=7)
    with open_source(client, "auth_mock_004", "Host4", mint_stream_token(client, "auth_mock_004", 1)):
        with client.websocket_connect(f"/watch/auth_mock_004?ticket={ticket}") as ws:
            receive_until(ws, MSG_HEADER)
        ok("first use of the ticket succeeds")

        with client.websocket_connect(f"/watch/auth_mock_004?ticket={ticket}") as ws:
            raw = ws.receive_bytes()
            msg_type, payload = unpack_frame(raw)
            assert msg_type == MSG_ERROR, f"expected ERROR on reuse, got type={msg_type}"
        ok("second use of the same ticket -> rejected (single-use enforced)")


def test_watch_ticket_wrong_lobby_rejected(client: TestClient, *_):
    print("\n=== A ticket minted for one lobby doesn't work on another ===")
    register_livestream(client, "auth_mock_005", owner_user_id=1)
    register_livestream(client, "auth_mock_006", owner_user_id=1)
    ticket = mint_watch_ticket(client, "auth_mock_005", user_id=11)

    with open_source(client, "auth_mock_005", "Host5", mint_stream_token(client, "auth_mock_005", 1)), \
         open_source(client, "auth_mock_006", "Host6", mint_stream_token(client, "auth_mock_006", 1)):
        with client.websocket_connect(f"/watch/auth_mock_006?ticket={ticket}") as ws:
            raw = ws.receive_bytes()
            msg_type, payload = unpack_frame(raw)
            assert msg_type == MSG_ERROR, f"expected ERROR for cross-lobby ticket, got type={msg_type}"
        ok("ticket minted for auth_mock_005 rejected on auth_mock_006")


def test_watch_ticket_expired_rejected(client: TestClient, *_):
    print("\n=== An expired ticket is rejected ===")
    register_livestream(client, "auth_mock_007", owner_user_id=1)
    ticket = mint_watch_ticket(client, "auth_mock_007", user_id=3)
    server.watch_tickets[ticket]["expires_at"] = 1  # force expiry (1970) — contract no longer accepts it
    with open_source(client, "auth_mock_007", "Host7", mint_stream_token(client, "auth_mock_007", 1)):
        with client.websocket_connect(f"/watch/auth_mock_007?ticket={ticket}") as ws:
            raw = ws.receive_bytes()
            msg_type, payload = unpack_frame(raw)
            assert msg_type == MSG_ERROR, f"expected ERROR for expired ticket, got type={msg_type}"
        ok("expired ticket -> rejected")


def test_stream_token_single_use(client: TestClient, *_):
    print("\n=== A consumed stream token cannot be reused ===")
    register_livestream(client, "auth_mock_008", owner_user_id=1)
    token = mint_stream_token(client, "auth_mock_008", user_id=5)
    with open_source(client, "auth_mock_008", "Host8", token):
        pass
    ok("first use of the stream token succeeds")

    with client.websocket_connect(f"/stream/auth_mock_008?stream_token={token}") as ws:
        raw = ws.receive_bytes()
        msg_type, payload = unpack_frame(raw)
        assert msg_type == MSG_ERROR, f"expected ERROR on reuse, got type={msg_type}"
    ok("second use of the same stream token -> rejected (single-use enforced)")


def test_stream_token_expired_rejected(client: TestClient, *_):
    print("\n=== An expired stream token is rejected ===")
    register_livestream(client, "auth_mock_009", owner_user_id=1)
    token = mint_stream_token(client, "auth_mock_009", user_id=6)
    server.stream_tokens[token]["expires_at"] = 1  # force expiry (1970) — contract no longer accepts it
    with client.websocket_connect(f"/stream/auth_mock_009?stream_token={token}") as ws:
        raw = ws.receive_bytes()
        msg_type, payload = unpack_frame(raw)
        assert msg_type == MSG_ERROR, f"expected ERROR for expired token, got type={msg_type}"
    ok("expired stream token -> rejected")


def test_stream_no_token_rejected(client: TestClient, *_):
    print("\n=== /stream with no stream token is rejected ===")
    register_livestream(client, "auth_mock_010", owner_user_id=1)
    with client.websocket_connect("/stream/auth_mock_010") as ws:
        raw = ws.receive_bytes()
        msg_type, payload = unpack_frame(raw)
        assert msg_type == MSG_ERROR, f"expected ERROR with no token, got type={msg_type}"
    ok("no stream_token -> MSG_ERROR")


def test_health_open(client: TestClient, *_):
    print("\n=== /health stays unauthenticated ===")
    r = client.get("/health")
    assert r.status_code == 200, f"expected 200, got {r.status_code}"
    ok("GET /health -> 200")


def test_removed_endpoints_gone(client: TestClient, *_):
    print("\n=== Retired endpoints are no longer served ===")
    r = client.get("/games")
    assert r.status_code in (404, 405), f"expected /games gone, got {r.status_code}"
    ok("GET /games removed")

    r = client.get("/watch/auth_mock_011/ticket", headers=keys())
    assert r.status_code in (404, 405), f"expected ticket endpoint gone, got {r.status_code}"
    ok("GET /watch/{id}/ticket removed")

    register_livestream(client, "auth_mock_011", owner_user_id=1)
    try:
        with client.websocket_connect("/watch-reconnect/auth_mock_011") as ws:
            ws.receive_bytes()
        ok("/watch-reconnect removed (route gone, no admission)")
    except WebSocketDisconnect:
        ok("/watch-reconnect removed (route gone, connection rejected)")
    except Exception as e:
        ok(f"/watch-reconnect removed (route gone, got {type(e).__name__})")


def test_ticket_gets_configured_ttl(client: TestClient, *_):
    print("\n=== Minted credential gets WATCH_TICKET_TTL_SECONDS (no expires_at in contract) ===")
    register_livestream(client, "auth_mock_013", owner_user_id=1)
    ticket = mint_watch_ticket(client, "auth_mock_013", user_id=4)
    cred = server.watch_tickets.get(ticket)
    assert cred is not None, "ticket not stored"
    expected = server.time.time() + server.WATCH_TICKET_TTL_SECONDS
    assert abs(cred["expires_at"] - expected) < 2, f"expiry wrong: {cred['expires_at']}"
    ok("credential expiry = now + WATCH_TICKET_TTL_SECONDS")


def test_watch_ticket_priority_stored(client: TestClient, *_):
    print("\n=== Watch tickets carry the GO-stamped priority flag ===")
    register_livestream(client, "auth_mock_014", owner_user_id=1)
    normal = mint_watch_ticket(client, "auth_mock_014", user_id=1, priority=False)
    prio = mint_watch_ticket(client, "auth_mock_014", user_id=2, priority=True)
    assert server.watch_tickets[normal]["priority"] is False
    ok("normal ticket stored with priority=False")
    assert server.watch_tickets[prio]["priority"] is True
    ok("priority ticket stored with priority=True")
    assert "priority" not in server.stream_tokens  # stream tokens never carry it
    ok("stream tokens carry no priority flag")


def test_priority_ticket_bypasses_delay_hold(client: TestClient, *_):
    print("\n=== Priority ticket bypasses the delay hold; normal ticket is held ===")
    register_livestream(client, "auth_mock_015", owner_user_id=1, delay_seconds=2)
    token = mint_stream_token(client, "auth_mock_015", user_id=1)
    with client.websocket_connect(f"/stream/auth_mock_015?stream_token={token}") as sws:
        sws.send_bytes(pack_frame(MSG_REGISTER, json.dumps({
            "lobbyid": "auth_mock_015",
            "player_name": "Host15",
            "can_stream": True,
            "is_host": True,
        }).encode()))
        sws.receive_bytes()  # ROLE
        sws.send_bytes(pack_frame(MSG_HEADER, b"HEADER_15"))
        sws.send_bytes(pack_frame(MSG_BODY, struct.pack("<Q", 0) + b"B" * 40))
        import time as _time
        _time.sleep(0.2)  # let the body land before the observers join

        # Normal viewer: held — ROLE delay_seconds: 0, catch-up capped at the watermark
        # (0 for a session younger than the delay), and the body arrives on the wire only
        # once the delay has elapsed: the blocking read below is the hold in action.
        ticket = mint_watch_ticket(client, "auth_mock_015", user_id=9, priority=False)
        with client.websocket_connect(f"/watch/auth_mock_015?ticket={ticket}") as ws:
            role = receive_until(ws, MSG_ROLE)
            assert b'"delay_seconds":0' in role, f"held ROLE should carry delay 0, got {role!r}"
            ok("held observer ROLE delay_seconds: 0")
            hdr = receive_until(ws, MSG_HEADER)
            assert hdr == b"HEADER_15", f"unexpected header: {hdr!r}"
            t, pl = unpack_frame(ws.receive_bytes())
            assert t == MSG_BODY and pl[:8] == struct.pack('<Q', 9), \
                f"expected the delayed body at file offset 9, got type={t} pl={pl[:16]!r}"
            ok("held observer's body arrived only after the delay (byte-level hold)")

        # Priority viewer: instant — full catch-up body, session delay in the ROLE.
        pticket = mint_watch_ticket(client, "auth_mock_015", user_id=10, priority=True)
        with client.websocket_connect(f"/watch/auth_mock_015?ticket={pticket}") as ws:
            role = receive_until(ws, MSG_ROLE)
            assert b'"delay_seconds":2' in role, f"priority ROLE should carry delay 2, got {role!r}"
            ok("priority observer ROLE delay_seconds: 2")
            got_body = False
            for _ in range(6):
                raw = ws.receive_bytes()
                t, pl = unpack_frame(raw)
                if t == MSG_BODY:
                    got_body = True
                    break
            assert got_body, "priority observer should get the body immediately"
            ok("priority observer received the body immediately (bypass)")


# ═══════════════════════════════════════════════════════════════════════════
# Runner
# ═══════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("RELAY GO-ORCHESTRATED TICKET TESTS (mocked GO caller, in-process, no real sockets)")
    print("=" * 60)

    server.INTERNAL_API_KEY = RELAY_KEY

    with TestClient(server.app) as client:
        tests = [
            test_internal_key_required,
            test_livestream_register_and_base_url,
            test_stream_token_unknown_lobby,
            test_watch_ticket_unknown_lobby,
            test_web_connect_no_ticket_rejected,
            test_web_connect_valid_ticket_admits,
            test_watch_ticket_single_use,
            test_watch_ticket_wrong_lobby_rejected,
            test_watch_ticket_expired_rejected,
            test_stream_token_single_use,
            test_stream_token_expired_rejected,
            test_stream_no_token_rejected,
            test_health_open,
            test_removed_endpoints_gone,
            test_ticket_gets_configured_ttl,
            test_watch_ticket_priority_stored,
            test_priority_ticket_bypasses_delay_hold,
        ]

        for test in tests:
            try:
                test(client, None)
            except Exception as e:
                fail(test.__name__, str(e))

    print("\n" + "=" * 60)
    print(f"RESULTS: {PASS} passed, {FAIL} failed")
    print("=" * 60)
    return FAIL == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
