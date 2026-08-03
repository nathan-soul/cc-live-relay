#!/usr/bin/env python3
"""
Integration tests for the relay server (v0.4.0 binary protocol).
Tests: binary registration, envelope streaming, observer catchup, dual source dedup, /games endpoint.
"""
import asyncio
import json
import struct
import sys
import time
import websockets

BASE = "ws://localhost:8765"
HTTP = "http://localhost:8765"

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
            ok("health returns correct structure (v0.4.0)")


async def test_games_empty():
    print("\n=== /games (empty) ===")
    import aiohttp
    async with aiohttp.ClientSession() as s:
        async with s.get(f"{HTTP}/games") as r:
            data = await r.json()
            assert isinstance(data, list)
            assert len(data) == 0
            ok("games returns empty list initially")


async def test_register_as_source():
    print("\n=== Register as source (binary protocol) ===")
    async with websockets.connect(f"{BASE}/register") as ws:
        # Send binary REGISTER frame
        reg_json = json.dumps({
            "game_hash": "test_game_001",
            "player_name": "TestPlayer",
            "can_stream": True,
        })
        await ws.send(pack_frame(MSG_REGISTER, reg_json.encode()))

        # Receive binary ROLE response
        raw = await ws.recv()
        msg_type, payload = unpack_frame(raw)
        assert msg_type == MSG_ROLE, f"Expected ROLE (5), got {msg_type}"
        role_data = json.loads(payload.decode())
        assert role_data["role"] == "streamer"
        assert role_data["game_id"] == "test_game_001"
        ok("source gets role=streamer via binary ROLE frame")

        # Send HEADER
        await ws.send(pack_frame(MSG_HEADER, b"TEST_HEADER_PAYLOAD_v1"))
        ok("source sent HEADER")

        # Send BODY with offset=0
        body_data = b"\x01\x02\x03\x04" * 25
        body_payload = struct.pack('<Q', 0) + body_data
        await ws.send(pack_frame(MSG_BODY, body_payload))
        ok("source sent BODY offset=0")

        # Send another BODY with offset=100
        body_data2 = b"\x05\x06\x07\x08" * 25
        body_payload2 = struct.pack('<Q', 100) + body_data2
        await ws.send(pack_frame(MSG_BODY, body_payload2))
        ok("source sent BODY offset=100")

        # Send END
        await ws.send(pack_frame(MSG_END, b""))
        ok("source sent END")
        await asyncio.sleep(0.2)


async def test_observer_receives_data():
    print("\n=== Observer receives binary data ===")
    async with websockets.connect(f"{BASE}/register") as sws:
        # Binary REGISTER
        reg_json = json.dumps({
            "game_hash": "test_game_002",
            "player_name": "Streamer1",
            "can_stream": True,
        })
        await sws.send(pack_frame(MSG_REGISTER, reg_json.encode()))
        raw = await sws.recv()
        msg_type, payload = unpack_frame(raw)
        assert msg_type == MSG_ROLE
        role = json.loads(payload.decode())
        assert role["role"] == "streamer"

        # Send HEADER
        await sws.send(pack_frame(MSG_HEADER, b"GAME_HEADER_002"))

        # Send 3 BODY chunks
        for i in range(3):
            offset = i * 100
            chunk = struct.pack('<Q', offset) + (bytes([i] * 100))
            await sws.send(pack_frame(MSG_BODY, chunk))
        await asyncio.sleep(0.2)

        # Connect as observer
        async with websockets.connect(f"{BASE}/watch/test_game_002") as ows:
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
            assert len(body_msgs) >= 1, f"Expected >=1 BODY chunks (catchup combines into <=256KB), got {len(body_msgs)}"
            ok(f"observer received {len(body_msgs)} BODY chunk(s)")

        # Send END
        await sws.send(pack_frame(MSG_END, b""))
        await asyncio.sleep(0.2)


async def test_observer_error_on_ended():
    print("\n=== Observer error on ended game ===")
    async with websockets.connect(f"{BASE}/register") as sws:
        reg_json = json.dumps({
            "game_hash": "test_game_003",
            "player_name": "Streamer2",
            "can_stream": True,
        })
        await sws.send(pack_frame(MSG_REGISTER, reg_json.encode()))
        raw = await sws.recv()
        msg_type, payload = unpack_frame(raw)
        assert msg_type == MSG_ROLE

        # Send END, then disconnect -> session should be ended
        await sws.send(pack_frame(MSG_END, b""))
        await asyncio.sleep(0.3)

    # Try to watch — game should be ended, expect ERROR frame
    try:
        async with websockets.connect(f"{BASE}/watch/test_game_003") as ows:
            raw = await asyncio.wait_for(ows.recv(), timeout=2.0)
            if isinstance(raw, bytes):
                t, pl = unpack_frame(raw)
                if t == MSG_ERROR:
                    ok("observer gets binary ERROR for ended game")
                else:
                    fail("expected ERROR frame", f"got type={t}")
            else:
                fail("expected binary ERROR", f"got text: {raw}")
    except Exception as e:
        ok(f"observer connection rejected for ended game")


async def test_games_list():
    print("\n=== /games (with active game) ===")
    async with websockets.connect(f"{BASE}/register") as sws:
        reg_json = json.dumps({
            "game_hash": "test_game_004",
            "player_name": "ListTest",
            "can_stream": True,
        })
        await sws.send(pack_frame(MSG_REGISTER, reg_json.encode()))
        raw = await sws.recv()
        msg_type, payload = unpack_frame(raw)
        assert msg_type == MSG_ROLE
        role = json.loads(payload.decode())
        assert role["role"] == "streamer"

        # Send HEADER
        await sws.send(pack_frame(MSG_HEADER, b"LIST_HEADER"))
        await asyncio.sleep(0.2)

        import aiohttp
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{HTTP}/games") as r:
                data = await r.json()
                assert isinstance(data, list)
                assert len(data) >= 1, f"Expected >=1 games, got {len(data)}"
                matching = [g for g in data if g.get("game_id") == "test_game_004"]
                assert len(matching) == 1, f"Expected test_game_004 in games list, got: {data}"
                game = matching[0]
                assert game.get("body_bytes") is not None, "body_bytes field missing"
                assert game.get("sources") is not None, "sources field missing"
                ok("/games lists active game with body_bytes + sources fields")

        # End
        await sws.send(pack_frame(MSG_END, b""))
        await asyncio.sleep(0.2)


async def test_dual_source_dedup():
    print("\n=== Dual source (dedup, no failover) ===")
    # Register source A
    sws_a = await websockets.connect(f"{BASE}/register")
    await sws_a.send(pack_frame(MSG_REGISTER, json.dumps({
        "game_hash": "test_game_005",
        "player_name": "SourceA",
        "can_stream": True,
    }).encode()))
    raw = await sws_a.recv()
    msg_type, payload = unpack_frame(raw)
    assert msg_type == MSG_ROLE
    role_a = json.loads(payload.decode())
    assert role_a["role"] == "streamer"
    ok("source A registered as streamer")

    # Register source B (should ALSO be streamer, not backup!)
    sws_b = await websockets.connect(f"{BASE}/register")
    await sws_b.send(pack_frame(MSG_REGISTER, json.dumps({
        "game_hash": "test_game_005",
        "player_name": "SourceB",
        "can_stream": True,
    }).encode()))
    raw = await sws_b.recv()
    msg_type, payload = unpack_frame(raw)
    assert msg_type == MSG_ROLE
    role_b = json.loads(payload.decode())
    assert role_b["role"] == "streamer", f"Expected streamer, got {role_b['role']}"
    ok("source B also registered as streamer (no backup role)")

    # Both send HEADER (same data)
    header = b"DUAL_HEADER_005"
    await sws_a.send(pack_frame(MSG_HEADER, header))
    await sws_b.send(pack_frame(MSG_HEADER, header))
    ok("both sources sent HEADER")

    # Both send BODY offset=0 with same data
    body1 = b"A" * 200
    body_payload1 = struct.pack('<Q', 0) + body1
    await sws_a.send(pack_frame(MSG_BODY, body_payload1))
    await sws_b.send(pack_frame(MSG_BODY, body_payload1))
    await asyncio.sleep(0.2)
    ok("both sources sent same BODY offset=0 (relay deduplicates)")

    # Connect observer
    ows = await websockets.connect(f"{BASE}/watch/test_game_005")
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

    # Source B sends more body data — should reach observer (no takeover needed)
    body2 = b"B" * 150
    body_payload2 = struct.pack('<Q', 200) + body2
    await sws_b.send(pack_frame(MSG_BODY, body_payload2))
    await asyncio.sleep(0.5)

    # Observer should receive the new data
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

    # Cleanup
    await sws_b.send(pack_frame(MSG_END, b""))
    await asyncio.sleep(0.2)
    await sws_b.close()
    await ows.close()
    await asyncio.sleep(0.2)


async def test_reconnect_with_offset():
    print("\n=== Reconnect with last_offset ===")
    async with websockets.connect(f"{BASE}/register") as sws:
        await sws.send(pack_frame(MSG_REGISTER, json.dumps({
            "game_hash": "test_game_006",
            "player_name": "ReconnectSource",
            "can_stream": True,
        }).encode()))
        raw = await sws.recv()
        msg_type, payload = unpack_frame(raw)
        assert msg_type == MSG_ROLE
        role = json.loads(payload.decode())
        assert role["role"] == "streamer"

        # Send HEADER
        await sws.send(pack_frame(MSG_HEADER, b"RECONNECT_HEADER"))

        # Send BODY at offset 0
        body_data = b"X" * 300
        await sws.send(pack_frame(MSG_BODY, struct.pack('<Q', 0) + body_data))
        await asyncio.sleep(0.2)

        # Reconnect with last_offset=150
        async with websockets.connect(f"{BASE}/watch-reconnect/test_game_006") as rws:
            await rws.send(json.dumps({
                "type": "reconnect",
                "last_offset": 150,
            }))
            ack = json.loads(await rws.recv())
            assert ack["type"] == "reconnect"
            assert "server_body_bytes" in ack
            ok("reconnect ack received with server_body_bytes")

            msgs = []
            try:
                for _ in range(15):
                    raw = await asyncio.wait_for(rws.recv(), timeout=1.0)
                    msgs.append(raw)
            except asyncio.TimeoutError:
                pass

            body_bin = [m for m in msgs if isinstance(m, bytes)]
            ok(f"reconnect observer received {len(body_bin)} binary messages")

        # End
        await sws.send(pack_frame(MSG_END, b""))
        await asyncio.sleep(0.2)


async def test_debug_body():
    print("\n=== /debug/body endpoint ===")
    async with websockets.connect(f"{BASE}/register") as sws:
        await sws.send(pack_frame(MSG_REGISTER, json.dumps({
            "game_hash": "test_game_007",
            "player_name": "DebugTest",
            "can_stream": True,
        }).encode()))
        raw = await sws.recv()
        msg_type, payload = unpack_frame(raw)
        assert msg_type == MSG_ROLE

        await sws.send(pack_frame(MSG_HEADER, b"DEBUG_HEADER"))
        await sws.send(pack_frame(MSG_BODY, struct.pack('<Q', 0) + b"DEBUG_BODY_DATA_12345"))
        await asyncio.sleep(0.2)

        import aiohttp
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{HTTP}/debug/body/test_game_007") as r:
                data = await r.json()
                assert "error" not in data, f"debug/body returned error: {data}"
                assert data.get("game_id") == "test_game_007"
                assert data.get("body_bytes", 0) > 0, f"body_bytes should be > 0, got: {data}"
                assert data.get("header_bytes", 0) > 0, f"header_bytes should be > 0, got: {data}"
                ok("debug/body returns body_bytes + header_bytes")

        await sws.send(pack_frame(MSG_END, b""))
        await asyncio.sleep(0.2)


async def test_register_error_missing_hash():
    print("\n=== Register error: missing game_hash ===")
    async with websockets.connect(f"{BASE}/register") as ws:
        await ws.send(pack_frame(MSG_REGISTER, json.dumps({
            "player_name": "BadClient",
            "can_stream": True,
        }).encode()))
        raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
        if isinstance(raw, bytes):
            t, pl = unpack_frame(raw)
            if t == MSG_ERROR:
                ok("got ERROR frame for missing game_hash")
            else:
                fail("expected ERROR", f"got type={t}")
        else:
            fail("expected binary ERROR", f"got text: {raw}")


# ═══════════════════════════════════════════════════════════════════════════
# Runner
# ═══════════════════════════════════════════════════════════════════════════

async def main():
    print("=" * 60)
    print("RELAY SERVER INTEGRATION TESTS (v0.4.0 binary protocol)")
    print("=" * 60)

    tests = [
        test_health,
        test_games_empty,
        test_register_as_source,
        test_observer_receives_data,
        test_observer_error_on_ended,
        test_games_list,
        test_dual_source_dedup,
        test_reconnect_with_offset,
        test_debug_body,
        test_register_error_missing_hash,
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
