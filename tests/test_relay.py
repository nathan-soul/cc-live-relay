#!/usr/bin/env python3
"""
Integration tests for the relay server (v0.5.0 binary protocol).
Tests: binary registration over /stream with a GO-minted token, envelope streaming,
observer catchup over /watch with a GO-minted ticket, dual source dedup, internal
endpoint auth.

Requires a running relay:  python server.py   (PORT env or default 8765)
Run:                      python tests/test_relay.py
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
MSG_CHAT     = 7
MSG_SPECTATOR_CHAT = 8


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


async def register_livestream(lobby_id, owner_user_id=1, delay_seconds=None):
    payload = {"lobby_id": lobby_id, "owner_user_id": owner_user_id}
    if delay_seconds is not None:
        payload["delay_seconds"] = delay_seconds
    return await internal_post("/internal/livestreams", payload)


async def mint_stream_token(lobby_id, user_id=1):
    data = await internal_post("/internal/stream_tokens", {"lobby_id": lobby_id, "user_id": user_id})
    return data["url"].split("stream_token=")[1]


async def mint_watch_ticket(lobby_id, user_id=9, priority=False):
    data = await internal_post("/internal/watch_tickets",
                               {"lobby_id": lobby_id, "user_id": user_id, "priority": priority})
    return data["url"].split("ticket=")[1]


async def connect_source(lobby_id, user_id=1, is_observer=False):
    """Connect a source to /stream with a freshly minted token, do the REGISTER handshake."""
    token = await mint_stream_token(lobby_id, user_id)
    ws = await websockets.connect(f"{BASE}/stream/{lobby_id}?stream_token={token}")
    await ws.send(pack_frame(MSG_REGISTER, json.dumps({
        "lobbyid": lobby_id,
        "player_name": f"Source{user_id}",
        "can_stream": True,
        "is_host": True,
        "is_observer": is_observer,
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

    # Priority ticket (no delay hold): the observer must get the body immediately.
    ticket = await mint_watch_ticket("test_game_003", user_id=9, priority=True)
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

    ticket = await mint_watch_ticket("test_game_009", user_id=9, priority=True)
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


# ── Byte-level delay hold (plans/relay/relay-server-side-delay-hold.md) ──────

async def test_held_observer_delay_edge():
    print("\n=== Held observer: body arrives only after the broadcast delay ===")
    await register_livestream("test_hold_001", owner_user_id=1, delay_seconds=5)
    sws = await connect_source("test_hold_001", user_id=1)
    await sws.send(pack_frame(MSG_HEADER, b"HEADER_HOLD"))
    await sws.send(pack_frame(MSG_BODY, struct.pack('<Q', 0) + b"H" * 60))
    await asyncio.sleep(0.2)

    ticket = await mint_watch_ticket("test_hold_001", user_id=9, priority=False)
    async with websockets.connect(f"{BASE}/watch/test_hold_001?ticket={ticket}") as ows:
        role = None
        for _ in range(5):
            raw = await asyncio.wait_for(ows.recv(), timeout=2.0)
            if isinstance(raw, bytes):
                t, pl = unpack_frame(raw)
                if t == MSG_ROLE:
                    role = pl.decode()
                    break
        assert role is not None and '"delay_seconds":0' in role, f"held ROLE wrong: {role!r}"
        ok("held observer ROLE delay_seconds: 0 (the data edge is the delay)")

        # Catch-up is capped at the watermark (0 for a session younger than the delay):
        # HEADER arrives instantly, then the socket is silent while the delay elapses —
        # the 5 s delay gives plenty of margin over the join latency itself.
        t, pl = unpack_frame(await asyncio.wait_for(ows.recv(), timeout=2.0))
        assert t == MSG_HEADER, f"expected catch-up HEADER, got type={t}"
        try:
            await asyncio.wait_for(ows.recv(), timeout=1.0)
            raise AssertionError("held observer got a frame within 1s of joining")
        except asyncio.TimeoutError:
            ok("held observer silent while the delay elapses")
        t, pl = unpack_frame(await asyncio.wait_for(ows.recv(), timeout=8.0))
        assert t == MSG_BODY, f"expected the delayed body, got type={t}"
        offset = struct.unpack('<Q', pl[:8])[0]
        assert offset == len(b"HEADER_HOLD"), f"unexpected file offset {offset}"
        ok("held observer's body arrived once the delay elapsed")

    # Priority observer on the same stream: instant full catch-up.
    pticket = await mint_watch_ticket("test_hold_001", user_id=10, priority=True)
    async with websockets.connect(f"{BASE}/watch/test_hold_001?ticket={pticket}") as ows:
        role = None
        got_body = False
        for _ in range(10):
            raw = await asyncio.wait_for(ows.recv(), timeout=2.0)
            if isinstance(raw, bytes):
                t, pl = unpack_frame(raw)
                if t == MSG_ROLE:
                    role = pl.decode()
                elif t == MSG_BODY:
                    got_body = True
                    break
        assert role is not None and '"delay_seconds":5' in role, f"priority ROLE wrong: {role!r}"
        assert got_body, "priority observer should get the body immediately"
        ok("priority observer: full body immediately, delay_seconds: 5")

    await sws.send(pack_frame(MSG_END, b""))
    await asyncio.sleep(0.2)
    await sws.close()


# ── Chat (MSG_CHAT / MSG_SPECTATOR_CHAT) ───────────────────────────────────

async def test_player_chat_roundtrip_dedup_history():
    print("\n=== Player chat (MSG_CHAT): live broadcast, dedup, late-join history slice ===")
    await register_livestream("test_chat_001")
    sws_a = await connect_source("test_chat_001", user_id=1)
    sws_b = await connect_source("test_chat_001", user_id=2)
    await sws_a.send(pack_frame(MSG_HEADER, b"CHAT_HEADER_001"))
    await asyncio.sleep(0.2)

    # All-push: two sources forward byte-identical copies of the same chat frame.
    chat1 = struct.pack('<II', 100, 5) + b"hello" + struct.pack('<I', 0x00FF00)
    await sws_a.send(pack_frame(MSG_CHAT, chat1))
    await sws_b.send(pack_frame(MSG_CHAT, chat1))
    await asyncio.sleep(0.3)

    # A watcher joining after the chat was sent must get it via the catch-up slice, once.
    ticket = await mint_watch_ticket("test_chat_001", user_id=9)
    async with websockets.connect(f"{BASE}/watch/test_chat_001?ticket={ticket}") as ows:
        chats = []
        try:
            for _ in range(15):
                raw = await asyncio.wait_for(ows.recv(), timeout=1.0)
                if isinstance(raw, bytes):
                    t, pl = unpack_frame(raw)
                    if t == MSG_CHAT:
                        chats.append(pl)
        except asyncio.TimeoutError:
            pass
        assert chats.count(chat1) == 1, f"expected exactly one copy of chat1, got {chats.count(chat1)}"
        ok("late-join observer received the chat history slice exactly once (deduped)")

    # A second late joiner sees both chats in the history.
    chat2 = struct.pack('<II', 200, 5) + b"world" + struct.pack('<I', 0x0000FF)
    await sws_a.send(pack_frame(MSG_CHAT, chat2))
    await asyncio.sleep(0.2)
    ticket2 = await mint_watch_ticket("test_chat_001", user_id=10)
    async with websockets.connect(f"{BASE}/watch/test_chat_001?ticket={ticket2}") as ows2:
        chats2 = []
        try:
            for _ in range(15):
                raw = await asyncio.wait_for(ows2.recv(), timeout=1.0)
                if isinstance(raw, bytes):
                    t, pl = unpack_frame(raw)
                    if t == MSG_CHAT:
                        chats2.append(pl)
        except asyncio.TimeoutError:
            pass
        assert chat1 in chats2 and chat2 in chats2, f"history should contain both chats, got {len(chats2)}"
        ok("catch-up history slice contains both prior chats")

    await sws_a.send(pack_frame(MSG_END, b""))
    await sws_b.send(pack_frame(MSG_END, b""))
    await asyncio.sleep(0.2)
    await sws_a.close()
    await sws_b.close()


async def test_spectator_chat_audience():
    print("\n=== Spectator chat (MSG_SPECTATOR_CHAT): watchers + observer-mode sources only ===")
    await register_livestream("test_chat_002")
    player_src = await connect_source("test_chat_002", user_id=1)
    obs_src = await connect_source("test_chat_002", user_id=2, is_observer=True)
    await player_src.send(pack_frame(MSG_HEADER, b"CHAT_HEADER_002"))
    await asyncio.sleep(0.2)

    ticket1 = await mint_watch_ticket("test_chat_002", user_id=9)
    ticket2 = await mint_watch_ticket("test_chat_002", user_id=10)
    w1 = await websockets.connect(f"{BASE}/watch/test_chat_002?ticket={ticket1}")
    w2 = await websockets.connect(f"{BASE}/watch/test_chat_002?ticket={ticket2}")
    for ws in (w1, w2):
        try:
            for _ in range(10):
                await asyncio.wait_for(ws.recv(), timeout=1.0)
        except asyncio.TimeoutError:
            pass

    spec = struct.pack('<I', 4) + b"Spec" + struct.pack('<I', 5) + b"hello"
    await w1.send(pack_frame(MSG_SPECTATOR_CHAT, spec))
    await asyncio.sleep(0.3)

    got_w2 = None
    try:
        for _ in range(5):
            raw = await asyncio.wait_for(w2.recv(), timeout=1.0)
            if isinstance(raw, bytes):
                t, pl = unpack_frame(raw)
                if t == MSG_SPECTATOR_CHAT:
                    got_w2 = pl
                    break
    except asyncio.TimeoutError:
        pass
    assert got_w2 == spec, "watcher should receive spectator chat from another watcher"
    ok("watcher received spectator chat from another watcher")

    got_obs = None
    try:
        for _ in range(5):
            raw = await asyncio.wait_for(obs_src.recv(), timeout=1.0)
            t, pl = unpack_frame(raw)
            if t == MSG_SPECTATOR_CHAT:
                got_obs = pl
                break
    except asyncio.TimeoutError:
        pass
    assert got_obs == spec, "observer-mode source should receive spectator chat"
    ok("observer-mode source received spectator chat")

    got_player = None
    try:
        for _ in range(5):
            raw = await asyncio.wait_for(player_src.recv(), timeout=0.5)
            t, pl = unpack_frame(raw)
            if t == MSG_SPECTATOR_CHAT:
                got_player = pl
                break
    except asyncio.TimeoutError:
        pass
    assert got_player is None, "streaming player must not receive spectator chat"
    ok("streaming player did not receive spectator chat")

    # Late joiner gets no spectator-chat history ("you missed it").
    ticket3 = await mint_watch_ticket("test_chat_002", user_id=11)
    async with websockets.connect(f"{BASE}/watch/test_chat_002?ticket={ticket3}") as w3:
        got_hist = None
        try:
            for _ in range(15):
                raw = await asyncio.wait_for(w3.recv(), timeout=1.0)
                if isinstance(raw, bytes):
                    t, pl = unpack_frame(raw)
                    if t == MSG_SPECTATOR_CHAT:
                        got_hist = pl
                        break
        except asyncio.TimeoutError:
            pass
        assert got_hist is None, "late joiner must not get spectator chat history"
        ok("late-joining watcher got no spectator chat history")

    # A player source trying to send spectator chat is ignored (defence in depth).
    await player_src.send(pack_frame(MSG_SPECTATOR_CHAT, spec))
    await asyncio.sleep(0.2)
    got_spam = None
    try:
        for _ in range(5):
            raw = await asyncio.wait_for(w2.recv(), timeout=0.5)
            if isinstance(raw, bytes):
                t, pl = unpack_frame(raw)
                if t == MSG_SPECTATOR_CHAT:
                    got_spam = pl
                    break
    except asyncio.TimeoutError:
        pass
    assert got_spam is None, "player-source spectator chat must be ignored"
    ok("player-source spectator chat ignored (defence in depth)")

    for ws in (player_src, obs_src, w1, w2):
        await ws.close()


async def test_spectator_chat_rate_limit():
    print("\n=== Spectator chat rate limit ===")
    await register_livestream("test_chat_003")
    sws = await connect_source("test_chat_003", user_id=1)
    await sws.send(pack_frame(MSG_HEADER, b"H"))
    await asyncio.sleep(0.2)
    ticket = await mint_watch_ticket("test_chat_003", user_id=9)
    async with websockets.connect(f"{BASE}/watch/test_chat_003?ticket={ticket}") as w:
        try:
            for _ in range(10):
                await asyncio.wait_for(w.recv(), timeout=1.0)
        except asyncio.TimeoutError:
            pass

        received = 0
        for i in range(8):
            payload = struct.pack('<I', 1) + b"X" + struct.pack('<I', 1) + bytes([i])
            await w.send(pack_frame(MSG_SPECTATOR_CHAT, payload))
        await asyncio.sleep(0.4)
        try:
            for _ in range(30):
                raw = await asyncio.wait_for(w.recv(), timeout=1.0)
                if isinstance(raw, bytes):
                    t, pl = unpack_frame(raw)
                    if t == MSG_SPECTATOR_CHAT:
                        received += 1
        except asyncio.TimeoutError:
            pass
        assert received <= 5, f"expected <=5 delivered (window is 5/10s), got {received}"
        ok(f"rate limit held ({received} of 8 sent delivered)")
    await sws.close()


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
        test_held_observer_delay_edge,
        test_player_chat_roundtrip_dedup_history,
        test_spectator_chat_audience,
        test_spectator_chat_rate_limit,
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
