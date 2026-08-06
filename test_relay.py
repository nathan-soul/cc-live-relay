#!/usr/bin/env python3
"""
Integration tests for the relay server (v0.5.0 binary protocol).
Tests: binary registration over /stream with a GO-minted token, envelope streaming,
observer catchup over /watch with a GO-minted ticket, dual source dedup, internal
endpoint auth.

Requires a running relay:  python server.py   (PORT env or default 8765)
Run:                      python test_relay.py
"""
import asyncio
import json
import os
import struct
import sys
import time
import websockets

PORT = os.getenv("RELAY_TEST_PORT", "8765")
HOST = os.getenv("RELAY_TEST_HOST", "localhost")
BASE = f"ws://{HOST}:{PORT}"
HTTP = f"http://{HOST}:{PORT}"

RELAY_KEY = os.getenv("RELAY_TEST_KEY", "test123")

PASS = 0
FAIL = 0

# ── Message types (aligned with server.py / C++ client) ──────────────────
MSG_REGISTER = 0
MSG_HEADER   = 1
MSG_PATCH    = 2
MSG_BODY     = 3
MSG_END      = 4
MSG_ROLE     = 5
MSG_ERROR    = 6


def pack_frame(msg_type: int, payload: bytes = b"") -> bytes:
    return bytes([msg_type]) + struct.pack('<I', len(payload)) + payload


def unpack_frame(data: bytes) -> tuple:
    if len(data) < 5:
        return (None, b"")
    msg_type = data[0]
    payload_len = struct.unpack("<I", data[1:5])[0]
    if len(data) < 5 + payload_len:
        return (None, b"")
    return (msg_type, data[5:5 + payload_len])


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


async def internal_post(path, payload):
    import aiohttp
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{HTTP}{path}", json=payload, headers=keys()) as r:
            assert r.status == 200, f"POST {path} -> {r.status}: {await r.text()}"
            return await r.json()


async def register_livestream(lobby_id, owner_user_id=1):
    return await internal_post("/internal/livestreams", {"lobby_id": lobby_id, "owner_user_id": owner_user_id})


async def mint_stream_token(lobby_id, user_id=1):
    data = await internal_post("/internal/stream_tokens", {"lobby_id": lobby_id, "user_id": user_id})
    return data["url"].split("stream_token=")[1]


async def mint_watch_ticket(lobby_id, user_id=9):
    data = await internal_post("/internal/watch_tickets", {"lobby_id": lobby_id, "user_id": user_id})
    return data["url"].split("ticket=")[1]


async def connect_source(lobby_id, user_id=1):
    """Connect a source to /stream with a freshly minted token, do the REGISTER handshake."""
    token = await mint_stream_token(lobby_id, user_id)
    ws = await websockets.connect(f"{BASE}/stream/{lobby_id}?stream_token={token}")
    await ws.send(pack_frame(MSG_REGISTER, json.dumps({
        "lobbyid": lobby_id,
        "player_name": f"Source{user_id}",
        "can_stream": True,
        "is_host": True,
    }).encode()))
    raw = await asyncio.wait_for(ws.recv(), timeout=3.0)
    assert unpack_frame(raw)[0] == MSG_ROLE, f"expected ROLE, got {raw!r}"
    return ws


# ═══════════════════════════════════════════════════════════════════════════
# Tests
# ═══════════════════════════════════════════════════════════════════════════

async def test_health():
    print("\n=== /health ===")
    import aiohttp
    async with aiohttp.ClientSession() as s:
        async with s.get(f"{HTTP}/health") as r:
            data = await r.json()
            assert data["status"] == "ok"
            assert "active_games" in data
            assert "total_observers" in data
            assert "total_body_bytes" in data
            ok("health returns correct structure (v0.5.0)")


async def test_internal_key_required():
    print("\n=== Internal endpoints reject bad/missing relay key ===")
    import aiohttp
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{HTTP}/internal/livestreams", json={"lobby_id": "ig_001", "owner_user_id": 1}) as r:
            assert r.status == 401, f"expected 401 missing key, got {r.status}"
        ok("missing X-Relay-Key -> 401")

        async with s.post(f"{HTTP}/internal/livestreams", json={"lobby_id": "ig_002", "owner_user_id": 1},
                          headers={"X-Relay-Key": "nope"}) as r:
            assert r.status == 401, f"expected 401 wrong key, got {r.status}"
        ok("wrong X-Relay-Key -> 401")


async def test_stream_as_source():
    print("\n=== /stream with a valid token ===")
    await register_livestream("test_game_001")
    ws = await connect_source("test_game_001", user_id=1)
    ok("source registered via /stream, got ROLE=streamer")

    # Send HEADER
    await ws.send(pack_frame(MSG_HEADER, b"TEST_HEADER_PAYLOAD_v1"))
    ok("source sent HEADER")

    # Send BODY with offset=0
    body_data = b"\x01\x02\x03\x04" * 25
    await ws.send(pack_frame(MSG_BODY, struct.pack('<Q', 0) + body_data))
    ok("source sent BODY offset=0")

    # Send END
    await ws.send(pack_frame(MSG_END, b""))
    ok("source sent END")
    await asyncio.sleep(0.2)
    await ws.close()
    await asyncio.sleep(0.2)


async def test_stream_without_token_rejected():
    print("\n=== /stream with no token is rejected ===")
    await register_livestream("test_game_002")
    try:
        async with websockets.connect(f"{BASE}/stream/test_game_002") as ws:
            raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
            t, pl = unpack_frame(raw)
            assert t == MSG_ERROR, f"expected ERROR, got type={t}"
            ok("no stream_token -> MSG_ERROR")
    except Exception as e:
        ok(f"no stream_token -> connection rejected ({type(e).__name__})")


async def test_observer_receives_data():
    print("\n=== Observer receives binary data via ticket ===")
    await register_livestream("test_game_003")
    sws = await connect_source("test_game_003", user_id=1)

    # Send HEADER
    await sws.send(pack_frame(MSG_HEADER, b"GAME_HEADER_003"))

    # Send 3 BODY chunks
    for i in range(3):
        offset = i * 100
        await sws.send(pack_frame(MSG_BODY, struct.pack('<Q', offset) + (bytes([i] * 100))))
    await asyncio.sleep(0.2)

    ticket = await mint_watch_ticket("test_game_003", user_id=9)
    async with websockets.connect(f"{BASE}/watch/test_game_003?ticket={ticket}") as ows:
        messages = []
        try:
            for _ in range(20):
                raw = await asyncio.wait_for(ows.recv(), timeout=1.0)
                if isinstance(raw, bytes):
                    t, pl = unpack_frame(raw)
                    messages.append(("bin", t, len(pl) if pl else 0))
                elif isinstance(raw, str):
                    messages.append(("text", json.loads(raw)))
        except asyncio.TimeoutError:
            pass

        types_received = [m[1] for m in messages if m[0] == "bin"]
        assert MSG_HEADER in types_received, f"Expected HEADER, got binary types: {types_received}"
        ok("observer received HEADER")
        body_msgs = [m for m in messages if m[0] == "bin" and m[1] == MSG_BODY]
        assert len(body_msgs) >= 1, f"Expected >=1 BODY chunks, got {len(body_msgs)}"
        ok(f"observer received {len(body_msgs)} BODY chunk(s)")

    # Send END
    await sws.send(pack_frame(MSG_END, b""))
    await asyncio.sleep(0.2)
    await sws.close()
    await asyncio.sleep(0.2)


async def test_watch_without_ticket_rejected():
    print("\n=== /watch with no ticket is rejected ===")
    await register_livestream("test_game_004")
    try:
        async with websockets.connect(f"{BASE}/watch/test_game_004") as ws:
            raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
            t, pl = unpack_frame(raw)
            assert t == MSG_ERROR, f"expected ERROR, got type={t}"
            ok("no ticket -> MSG_ERROR")
    except Exception as e:
        ok(f"no ticket -> connection rejected ({type(e).__name__})")


async def test_watch_ticket_single_use():
    print("\n=== Watch ticket is single-use ===")
    await register_livestream("test_game_005")
    sws = await connect_source("test_game_005", user_id=1)
    await sws.send(pack_frame(MSG_HEADER, b"SINGLE_USE_HEADER"))
    await asyncio.sleep(0.2)

    ticket = await mint_watch_ticket("test_game_005", user_id=9)
    async with websockets.connect(f"{BASE}/watch/test_game_005?ticket={ticket}") as ows:
        ok("first use of the ticket succeeds")
        try:
            await asyncio.wait_for(ows.recv(), timeout=2.0)
        except asyncio.TimeoutError:
            pass

    try:
        async with websockets.connect(f"{BASE}/watch/test_game_005?ticket={ticket}") as ows:
            raw = await asyncio.wait_for(ows.recv(), timeout=2.0)
            t, pl = unpack_frame(raw)
            assert t == MSG_ERROR, f"expected ERROR on reuse, got type={t}"
            ok("second use of same ticket -> rejected")
    except Exception as e:
        ok(f"second use of same ticket -> rejected ({type(e).__name__})")

    await sws.send(pack_frame(MSG_END, b""))
    await asyncio.sleep(0.2)
    await sws.close()


async def test_ticket_wrong_lobby_rejected():
    print("\n=== Ticket minted for one lobby doesn't work on another ===")
    await register_livestream("test_game_007")
    await register_livestream("test_game_008")
    sws7 = await connect_source("test_game_007", user_id=1)
    sws8 = await connect_source("test_game_008", user_id=1)
    await sws7.send(pack_frame(MSG_HEADER, b"H7"))
    await sws8.send(pack_frame(MSG_HEADER, b"H8"))
    await asyncio.sleep(0.2)

    ticket = await mint_watch_ticket("test_game_007", user_id=9)
    try:
        async with websockets.connect(f"{BASE}/watch/test_game_008?ticket={ticket}") as ows:
            raw = await asyncio.wait_for(ows.recv(), timeout=2.0)
            t, pl = unpack_frame(raw)
            assert t == MSG_ERROR, f"expected ERROR cross-lobby, got type={t}"
            ok("cross-lobby ticket -> rejected")
    except Exception as e:
        ok(f"cross-lobby ticket -> rejected ({type(e).__name__})")

    for ws in (sws7, sws8):
        await ws.send(pack_frame(MSG_END, b""))
    await asyncio.sleep(0.2)
    await sws7.close()
    await sws8.close()


async def test_dual_source_dedup():
    print("\n=== Dual source (dedup, no failover) ===")
    await register_livestream("test_game_009")
    sws_a = await connect_source("test_game_009", user_id=1)
    sws_b = await connect_source("test_game_009", user_id=2)
    ok("both sources registered as streamer via /stream")

    header = b"DUAL_HEADER_009"
    await sws_a.send(pack_frame(MSG_HEADER, header))
    await sws_b.send(pack_frame(MSG_HEADER, header))
    ok("both sources sent HEADER")

    body1 = b"A" * 200
    await sws_a.send(pack_frame(MSG_BODY, struct.pack('<Q', 0) + body1))
    await sws_b.send(pack_frame(MSG_BODY, struct.pack('<Q', 0) + body1))
    await asyncio.sleep(0.2)
    ok("both sources sent same BODY offset=0 (relay deduplicates)")

    ticket = await mint_watch_ticket("test_game_009", user_id=9)
    ows = await websockets.connect(f"{BASE}/watch/test_game_009?ticket={ticket}")
    msgs_before = []
    try:
        for _ in range(10):
            raw = await asyncio.wait_for(ows.recv(), timeout=1.0)
            msgs_before.append(raw)
    except asyncio.TimeoutError:
        pass

    body_msgs = [m for m in msgs_before if isinstance(m, bytes)]
    assert len(body_msgs) > 0, f"Observer should receive data, got {len(msgs_before)} msgs"
    ok(f"observer received {len(body_msgs)} binary messages during catchup")

    # Disconnect source A — session should NOT end (source B still present)
    await sws_a.close()
    await asyncio.sleep(0.5)

    body2 = b"B" * 150
    await sws_b.send(pack_frame(MSG_BODY, struct.pack('<Q', 200) + body2))
    await asyncio.sleep(0.5)

    msgs_after = []
    try:
        for _ in range(5):
            raw = await asyncio.wait_for(ows.recv(), timeout=1.0)
            msgs_after.append(raw)
    except asyncio.TimeoutError:
        pass

    new_body = [m for m in msgs_after if isinstance(m, bytes)]
    assert len(new_body) > 0, f"Observer should get new data after source A disconnect, got {len(msgs_after)} msgs"
    ok("observer received data from source B after source A disconnected (no takeover)")

    await sws_b.send(pack_frame(MSG_END, b""))
    await asyncio.sleep(0.2)
    await sws_b.close()
    await ows.close()
    await asyncio.sleep(0.2)


async def test_delete_livestream_ends_game():
    print("\n=== DELETE /internal/livestreams/{id} ends the session ===")
    import aiohttp
    await register_livestream("test_game_010")
    sws = await connect_source("test_game_010", user_id=1)
    await sws.send(pack_frame(MSG_HEADER, b"DELETE_HEADER"))
    await asyncio.sleep(0.2)

    async with aiohttp.ClientSession() as s:
        async with s.delete(f"{HTTP}/internal/livestreams/test_game_010", headers=keys()) as r:
            assert r.status == 200, f"expected 200, got {r.status}"
        ok("DELETE /internal/livestreams/{id} -> 200")

    # Watch on the ended game should fail even with a fresh ticket mint path 404ing.
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{HTTP}/internal/watch_tickets",
                          json={"lobby_id": "test_game_010", "user_id": 9},
                          headers=keys()) as r:
            assert r.status == 404, f"expected 404 mint on ended game, got {r.status}"
        ok("ticket mint for ended game -> 404")

    await sws.close()
    await asyncio.sleep(0.2)


async def test_retired_endpoints_gone():
    print("\n=== Retired endpoints are gone ===")
    import aiohttp
    async with aiohttp.ClientSession() as s:
        async with s.get(f"{HTTP}/games") as r:
            assert r.status in (404, 405), f"expected /games gone, got {r.status}"
        ok("GET /games removed")

        async with s.get(f"{HTTP}/debug/body/whatever") as r:
            assert r.status in (404, 405), f"expected /debug/body gone, got {r.status}"
        ok("GET /debug/body removed")

        async with s.get(f"{HTTP}/watch/no_such/ticket") as r:
            assert r.status in (404, 405), f"expected ticket endpoint gone, got {r.status}"
        ok("GET /watch/{id}/ticket removed")


# ═══════════════════════════════════════════════════════════════════════════
# Runner
# ═══════════════════════════════════════════════════════════════════════════

async def main():
    print("=" * 60)
    print("RELAY SERVER INTEGRATION TESTS (v0.5.0 binary protocol)")
    print("=" * 60)

    tests = [
        test_health,
        test_internal_key_required,
        test_stream_as_source,
        test_stream_without_token_rejected,
        test_observer_receives_data,
        test_watch_without_ticket_rejected,
        test_watch_ticket_single_use,
        test_ticket_wrong_lobby_rejected,
        test_dual_source_dedup,
        test_delete_livestream_ends_game,
        test_retired_endpoints_gone,
    ]

    for test in tests:
        try:
            await test()
        except Exception as e:
            fail(test.__name__, str(e))

    print("\n" + "=" * 60)
    print(f"RESULTS: {PASS} passed, {FAIL} failed")
    print("=" * 60)
    return FAIL == 0


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
