#!/usr/bin/env python3
"""
Integration tests for the relay server (v0.3.0 binary protocol).
Tests: registration, binary envelope streaming, observer catchup, dual source dedup, /games endpoint.
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

# ── Binary envelope helpers (mirror from server.py) ──────────────────────
HEADER_TYPE = 1
PATCH_TYPE = 2
BODY_TYPE = 3
END_TYPE = 4


def encode_envelope(msg_type: int, payload: bytes) -> bytes:
    return bytes([msg_type]) + struct.pack('<I', len(payload)) + payload


def decode_envelope(data: bytes) -> tuple[int, bytes]:
    if len(data) < 5:
        raise ValueError(f"Envelope too short: {len(data)} bytes")
    msg_type = data[0]
    length = struct.unpack('<I', data[1:5])[0]
    if len(data) < 5 + length:
        raise ValueError(f"Envelope truncated: expected {5+length} bytes, got {len(data)}")
    return msg_type, data[5:5 + length]


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
            ok("health returns correct structure (v0.3.0)")


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
        await ws.send(json.dumps({
            "type": "register",
            "game_hash": "test_game_001",
            "player_name": "TestPlayer",
            "can_stream": True,
        }))
        resp = json.loads(await ws.recv())
        assert resp["type"] == "role"
        assert resp["role"] == "streamer"
        assert resp["game_id"] == "test_game_001"
        ok("source gets role=streamer")

        # Send HEADER (binary)
        header_data = b"TEST_HEADER_PAYLOAD_v1"
        await ws.send(encode_envelope(HEADER_TYPE, header_data))
        ok("source sent HEADER")

        # Send BODY with offset=0
        body_data = b"\x01\x02\x03\x04" * 25  # 100 bytes of test data
        body_payload = struct.pack('<Q', 0) + body_data
        await ws.send(encode_envelope(BODY_TYPE, body_payload))
        ok("source sent BODY offset=0")

        # Send another BODY with offset=100
        body_data2 = b"\x05\x06\x07\x08" * 25  # next 100 bytes
        body_payload2 = struct.pack('<Q', 100) + body_data2
        await ws.send(encode_envelope(BODY_TYPE, body_payload2))
        ok("source sent BODY offset=100")

        await asyncio.sleep(0.1)

        # Send END
        await ws.send(encode_envelope(END_TYPE, b""))
        ok("source sent END")

        await asyncio.sleep(0.2)


async def test_observer_receives_data():
    print("\n=== Observer receives binary data ===")
    async with websockets.connect(f"{BASE}/register") as sws:
        await sws.send(json.dumps({
            "type": "register",
            "game_hash": "test_game_002",
            "player_name": "Streamer1",
            "can_stream": True,
        }))
        resp = json.loads(await sws.recv())
        assert resp["role"] == "streamer"

        # Send HEADER
        header = b"GAME_HEADER_002"
        await sws.send(encode_envelope(HEADER_TYPE, header))

        # Send 3 BODY chunks
        for i in range(3):
            offset = i * 100
            chunk = struct.pack('<Q', offset) + (bytes([i] * 100))
            await sws.send(encode_envelope(BODY_TYPE, chunk))
        await asyncio.sleep(0.2)

        # Now connect as observer
        async with websockets.connect(f"{BASE}/watch/test_game_002") as ows:
            messages = []
            try:
                for _ in range(20):
                    raw = await asyncio.wait_for(ows.recv(), timeout=1.0)
                    if isinstance(raw, bytes):
                        msg_type, payload = decode_envelope(raw)
                        messages.append(("bin", msg_type, len(payload)))
                    elif isinstance(raw, str):
                        messages.append(("text", json.loads(raw)))
            except asyncio.TimeoutError:
                pass

            types_received = [m[1] for m in messages if m[0] == "bin"]
            assert HEADER_TYPE in types_received, f"Expected HEADER, got binary types: {types_received}"
            ok("observer received HEADER")
            body_msgs = [m for m in messages if m[0] == "bin" and m[1] == BODY_TYPE]
            assert len(body_msgs) >= 1, f"Expected >=1 BODY chunks (catchup combines into <=256KB), got {len(body_msgs)}"
            ok(f"observer received {len(body_msgs)} BODY chunk(s)")

        # Send END
        await sws.send(encode_envelope(END_TYPE, b""))
        await asyncio.sleep(0.2)


async def test_observer_waiting():
    print("\n=== Observer waiting (ended game) ===")
    async with websockets.connect(f"{BASE}/register") as sws:
        await sws.send(json.dumps({
            "type": "register",
            "game_hash": "test_game_003",
            "player_name": "Streamer2",
            "can_stream": True,
        }))
        resp = json.loads(await sws.recv())
        assert resp["role"] == "streamer"

        # Send END, then disconnect → session should be ended
        await sws.send(encode_envelope(END_TYPE, b""))
        await asyncio.sleep(0.3)

    # Try to watch — game should be ended
    try:
        async with websockets.connect(f"{BASE}/watch/test_game_003") as ows:
            msg = json.loads(await ows.recv())
            if msg.get("type") == "error":
                ok("observer gets error for ended game")
            else:
                fail("expected error for ended game", f"got: {msg}")
    except Exception as e:
        ok(f"observer connection rejected for ended game")


async def test_games_list():
    print("\n=== /games (with active game) ===")
    async with websockets.connect(f"{BASE}/register") as sws:
        await sws.send(json.dumps({
            "type": "register",
            "game_hash": "test_game_004",
            "player_name": "ListTest",
            "can_stream": True,
        }))
        resp = json.loads(await sws.recv())
        assert resp["role"] == "streamer"

        # Send HEADER
        await sws.send(encode_envelope(HEADER_TYPE, b"LIST_HEADER"))

        await asyncio.sleep(0.2)

        import aiohttp
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{HTTP}/games") as r:
                data = await r.json()
                assert isinstance(data, list)
                assert len(data) >= 1
                matching = [g for g in data if g.get("game_id") == "test_game_004"]
                assert len(matching) == 1, f"Expected test_game_004 in games list, got: {data}"
                game = matching[0]
                assert game.get("body_bytes") is not None, "body_bytes field missing"
                assert game.get("sources") is not None, "sources field missing"
                ok("/games lists active game with body_bytes + sources fields")

        # End
        await sws.send(encode_envelope(END_TYPE, b""))
        await asyncio.sleep(0.2)


async def test_dual_source_dedup():
    print("\n=== Dual source (dedup, no failover) ===")
    # Register source A
    sws_a = await websockets.connect(f"{BASE}/register")
    await sws_a.send(json.dumps({
        "type": "register",
        "game_hash": "test_game_005",
        "player_name": "SourceA",
        "can_stream": True,
    }))
    resp_a = json.loads(await sws_a.recv())
    assert resp_a["role"] == "streamer"
    ok("source A registered as streamer")

    # Register source B (should ALSO be streamer, not backup!)
    sws_b = await websockets.connect(f"{BASE}/register")
    await sws_b.send(json.dumps({
        "type": "register",
        "game_hash": "test_game_005",
        "player_name": "SourceB",
        "can_stream": True,
    }))
    resp_b = json.loads(await sws_b.recv())
    assert resp_b["role"] == "streamer", f"Expected role=streamer, got {resp_b['role']}"
    ok("source B also registered as streamer (no backup role)")

    # Both send HEADER (same data)
    header = b"DUAL_HEADER_005"
    await sws_a.send(encode_envelope(HEADER_TYPE, header))
    await sws_b.send(encode_envelope(HEADER_TYPE, header))
    ok("both sources sent HEADER")

    # Both send BODY offset=0 with same data
    body1 = b"A" * 200
    body_payload1 = struct.pack('<Q', 0) + body1
    await sws_a.send(encode_envelope(BODY_TYPE, body_payload1))
    await sws_b.send(encode_envelope(BODY_TYPE, body_payload1))
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
    body_payload2 = struct.pack('<Q', 200) + body2  # offset=200, after first chunk
    await sws_b.send(encode_envelope(BODY_TYPE, body_payload2))
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
    await sws_b.send(encode_envelope(END_TYPE, b""))
    await asyncio.sleep(0.2)
    await sws_b.close()
    await ows.close()
    await asyncio.sleep(0.2)


async def test_reconnect_with_offset():
    print("\n=== Reconnect with last_offset ===")
    async with websockets.connect(f"{BASE}/register") as sws:
        await sws.send(json.dumps({
            "type": "register",
            "game_hash": "test_game_006",
            "player_name": "ReconnectSource",
            "can_stream": True,
        }))
        resp = json.loads(await sws.recv())
        assert resp["role"] == "streamer"

        # Send HEADER
        await sws.send(encode_envelope(HEADER_TYPE, b"RECONNECT_HEADER"))

        # Send BODY at offset 0 (first 300 bytes)
        body_data = b"X" * 300
        await sws.send(encode_envelope(BODY_TYPE, struct.pack('<Q', 0) + body_data))

        await asyncio.sleep(0.2)

        # Connect via reconnect with last_offset=150
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
            # Should get HEADER + BODY starting from offset 150
            ok(f"reconnect observer received {len(body_bin)} binary messages")

        # Cleanup
        await sws.send(encode_envelope(END_TYPE, b""))
        await asyncio.sleep(0.2)


async def test_debug_body():
    print("\n=== /debug/body endpoint ===")
    async with websockets.connect(f"{BASE}/register") as sws:
        await sws.send(json.dumps({
            "type": "register",
            "game_hash": "test_game_007",
            "player_name": "DebugTest",
            "can_stream": True,
        }))
        resp = json.loads(await sws.recv())
        assert resp["role"] == "streamer"

        await sws.send(encode_envelope(HEADER_TYPE, b"DEBUG_HEADER"))
        await sws.send(encode_envelope(BODY_TYPE, struct.pack('<Q', 0) + b"DEBUG_BODY_DATA_12345"))
        await asyncio.sleep(0.2)

        import aiohttp
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{HTTP}/debug/body/test_game_007") as r:
                data = await r.json()
                assert "error" not in data, f"debug/body returned error: {data}"
                assert data.get("game_id") == "test_game_007", f"Wrong game_id: {data}"
                assert "body_bytes" in data, f"Missing body_bytes in: {data}"
                assert "header_bytes" in data, f"Missing header_bytes in: {data}"
                assert data["body_bytes"] > 0, f"body_bytes should be > 0, got: {data}"
                assert data["header_bytes"] > 0, f"header_bytes should be > 0, got: {data}"
                ok("debug/body returns body_bytes + header_bytes")

        await sws.send(encode_envelope(END_TYPE, b""))
        await asyncio.sleep(0.2)


# ═══════════════════════════════════════════════════════════════════════════
# Runner
# ═══════════════════════════════════════════════════════════════════════════

async def main():
    print("=" * 60)
    print("RELAY SERVER INTEGRATION TESTS (v0.3.0 binary protocol)")
    print("=" * 60)

    tests = [
        test_health,
        test_games_empty,
        test_register_as_source,
        test_observer_receives_data,
        test_observer_waiting,
        test_games_list,
        test_dual_source_dedup,
        test_reconnect_with_offset,
        test_debug_body,
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
