#!/usr/bin/env python3
"""
Full-stack integration test client for the GO-orchestrated livestream stack:

    GO services (container)  <-- CheckLogin / WS / Lobbies / Livestreams
         |
         | RelayClient (X-Relay-Key)
         v
    cc-live-relay (container)  <-- /stream (streamer) + /watch (observer)

Drives the real GameClient HTTP+WS flow against the local test stack:
  1. CheckLogin with the DEBUG-only "ILOVECODE" code -> session_token, ws_uri
  2. Open the GO websocket (registers the session so LobbyManager knows the user)
  3. PUT /Lobbies to create a lobby (host), then WS START_GAME -> INGAME
  4. POST /livestreams/register -> stream token URL (as the streamer)
  5. POST /livestreams (GET) -> list contains our lobby
  6. Connect relay /stream, send HEADER+BODY
  7. POST /observe/{lobby_id} -> watch ticket URL (as a second user)
  8. Connect relay /watch, receive HEADER+BODY

Requires the compose test stack up: docker compose -f docker-compose.test.yml up --build

Usage:  python tests/test_stack_client.py [--go http://localhost:8080] [--relay ws://localhost:8765]
"""
import argparse
import asyncio
import json
import struct
import sys
import time

import aiohttp
import websockets

# ── GO web socket message ids (mirrors EWebSocketMessageID) ────────────────
START_GAME = 13

# ── Relay binary message types (mirrors server.py / C++ client) ────────────
MSG_REGISTER = 0
MSG_HEADER   = 1
MSG_BODY     = 3
MSG_END      = 4
MSG_ROLE     = 5
MSG_ERROR    = 6

PASS = 0
FAIL = 0


def ok(name):
    global PASS
    PASS += 1
    print(f"  [PASS] {name}")


def fail(name, reason=""):
    global FAIL
    FAIL += 1
    print(f"  [FAIL] {name} -- {reason}")


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


async def check_login(go_http: str) -> dict:
    """CheckLogin with the DEBUG-only ILOVECODE dev code."""
    body = {
        "code": "ILOVECODE",
        "client_id": "gen_online_30hz",
        "reserved_0": "",
        "reserved_1": "",
        "reserved_2": "",
        "exe_crc": "0",
        "ini_crc": "0",
    }
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{go_http}/env/prod/contract/1/CheckLogin", json=body) as r:
            text = await r.text()
            data = json.loads(text)
            if r.status != 200 or data.get("result") != 1:
                raise RuntimeError(f"CheckLogin failed ({r.status}): {text}")
            return data


async def open_go_ws(ws_uri: str, session_token: str, user_id: int):
    """Open the GO websocket, registering the user's session (needed by LobbyManager)."""
    # CheckLogin returns ws://localhost:8080/ws, but localhost may resolve to IPv6 ::1 where
    # Docker Desktop doesn't publish — force 127.0.0.1 to reach the container from this host.
    if "localhost" in ws_uri:
        ws_uri = ws_uri.replace("localhost", "127.0.0.1")
    headers = {
        "Authorization": f"Bearer {session_token}",
        "is-reconnect": "false",
        "client_id": "gen_online_30hz",
    }
    ws = await websockets.connect(ws_uri, additional_headers=headers,
                                  open_timeout=10, ping_interval=None)
    return ws


async def drain_go_ws(ws, label: str, stop: asyncio.Event, queue: asyncio.Queue):
    """Background reader: drains GO messages into a queue so the socket stays alive and
    multiple callers can consume from the single reader."""
    try:
        while not stop.is_set():
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except Exception:
                await queue.put(None)
                return
            try:
                await queue.put(json.loads(raw))
            except (json.JSONDecodeError, TypeError):
                pass
    except Exception:
        pass


async def create_lobby(go_http: str, session_token: str) -> int:
    """PUT /Lobbies as the host to create a lobby. Returns lobby_id."""
    body = {
        "name": "test-stack-lobby",
        "map_name": "test map",
        "map_path": "test map\\test map.map",
        "map_official": False,
        "max_players": 4,
        "preferred_port": 1234,
        "vanilla_teams": True,
        "track_stats": False,
        "starting_cash": 10000,
        "passworded": False,
        "password": "",
        "allow_observers": True,
        "max_cam_height": 3000.0,
        "exe_crc": 0,
        "ini_crc": 0,
        "anticheat_id": 0,
    }
    headers = {"Authorization": f"Bearer {session_token}"}
    async with aiohttp.ClientSession() as s:
        async with s.put(f"{go_http}/env/prod/contract/1/Lobbies", json=body, headers=headers) as r:
            text = await r.text()
            data = json.loads(text)
            if r.status != 200:
                raise RuntimeError(f"create lobby failed ({r.status}): {text}")
            lobby_id = data.get("lobby_id")
            if lobby_id is None or lobby_id < 0:
                raise RuntimeError(f"create lobby returned no lobby_id: {text}")
            return lobby_id


async def start_game(ws, lobby_id: int, queue: asyncio.Queue):
    """Send START_GAME over the GO websocket (host only) -> lobby becomes INGAME."""
    await ws.send(json.dumps({"msg_id": START_GAME}))
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            msg = await asyncio.wait_for(queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue
        if msg is None:
            return False
        if msg.get("msg_id") == START_GAME:
            return True
    return False


async def register_livestream(go_http: str, session_token: str) -> dict:
    headers = {"Authorization": f"Bearer {session_token}"}
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{go_http}/env/prod/contract/1/Livestreams/register",
                          headers=headers) as r:
            text = await r.text()
            data = json.loads(text)
            if r.status != 200:
                raise RuntimeError(f"livestreams/register failed ({r.status}): {text}")
            return data


async def list_livestreams(go_http: str, session_token: str) -> list:
    headers = {"Authorization": f"Bearer {session_token}"}
    async with aiohttp.ClientSession() as s:
        async with s.get(f"{go_http}/env/prod/contract/1/Livestreams", headers=headers) as r:
            text = await r.text()
            data = json.loads(text)
            if r.status != 200:
                raise RuntimeError(f"livestreams list failed ({r.status}): {text}")
            return data.get("livestreams", [])


async def observe(go_http: str, session_token: str, lobby_id: int) -> str:
    headers = {"Authorization": f"Bearer {session_token}"}
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{go_http}/env/prod/contract/1/Livestreams/observe/{lobby_id}",
                          headers=headers) as r:
            text = await r.text()
            data = json.loads(text)
            if r.status != 200:
                raise RuntimeError(f"observe failed ({r.status}): {text}")
            return data.get("url")


def rewrite_relay_host(url: str, relay_host: str) -> str:
    """Point a relay URL minted by GO at the given relay host.

    GO mints stream/watch URLs from PUBLIC_HOST (default localhost), which may not be reachable
    from the test machine (a stale local listener, IPv6 resolution, etc.). Rewrite only the
    authority (host[:port]) so the path/query — the part the relay routes on — is untouched.
    """
    from urllib.parse import urlsplit, urlunsplit
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, relay_host, parts.path, parts.query, parts.fragment))


async def stream_replay(relay_ws_url: str):
    """Connect as streamer to relay /stream, push HEADER + BODY."""
    ws = await websockets.connect(relay_ws_url, open_timeout=10, ping_interval=20)
    # REGISTER frame
    await ws.send(pack_frame(MSG_REGISTER, json.dumps({
        "lobbyid": "12345",  # overwritten by server from the URL path; sent for protocol shape
        "player_name": "test_streamer",
        "can_stream": True,
        "is_host": True,
    }).encode()))
    raw = await asyncio.wait_for(ws.recv(), timeout=10)
    t, pl = unpack_frame(raw)
    assert t == MSG_ROLE, f"streamer expected ROLE, got type={t}: {pl!r}"
    role = json.loads(pl.decode())
    ok(f"streamer registered, ROLE={role.get('role')}")

    # HEADER
    await ws.send(pack_frame(MSG_HEADER, b"FULL_STACK_HEADER"))
    # BODY
    body = struct.pack('<Q', 0) + b"B" * 4096
    await ws.send(pack_frame(MSG_BODY, body))
    ok("streamer sent HEADER + BODY")
    return ws


async def watch_replay(relay_watch_url: str):
    """Connect as observer to relay /watch, expect HEADER + BODY."""
    relay_watch_url = relay_watch_url.replace("localhost", "127.0.0.1")
    ws = await websockets.connect(relay_watch_url, open_timeout=10, ping_interval=20)
    got_header = False
    got_body = False
    for _ in range(20):
        raw = await asyncio.wait_for(ws.recv(), timeout=5)
        t, pl = unpack_frame(raw)
        if t == MSG_HEADER:
            got_header = True
        elif t == MSG_BODY:
            got_body = True
        if got_header and got_body:
            break
    ok(f"observer received HEADER={got_header} BODY={got_body}")
    assert got_header and got_body, "observer did not receive full replay"
    return ws


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--go", default="http://127.0.0.1:8080")
    ap.add_argument("--relay", default="ws://127.0.0.1:8765")
    args = ap.parse_args()

    # The relay host:port the test can actually reach (from --relay). GO mints stream/watch
    # URLs using its PUBLIC_HOST, which defaults to "localhost" — reachable in a normal Docker
    # setup but shadowable by a stale local listener. Rewriting to this host keeps the test
    # portable.
    relay_host = args.relay.split("://", 1)[-1].split("/", 1)[0]

    print("=" * 60)
    print("FULL-STACK LIVESTREAM TEST (GO + relay over real sockets)")
    print("=" * 60)

    # 1. Login streamer (user A)
    login_a = await check_login(args.go)
    token_a = login_a["session_token"]
    ws_uri = login_a["ws_uri"]
    user_a = login_a["user_id"]
    ok(f"CheckLogin user A -> user_id={user_a}, ws_uri={ws_uri}")

    # 2. Open GO WS for user A (registers the session)
    go_ws_a = await open_go_ws(ws_uri, token_a, user_a)
    ok("GO websocket session opened for user A")
    drain_stop = asyncio.Event()
    drain_queue = asyncio.Queue()
    drain_task = asyncio.create_task(drain_go_ws(go_ws_a, "A", drain_stop, drain_queue))
    await asyncio.sleep(1.0)

    # 3. Create lobby (host = user A)
    lobby_id = await create_lobby(args.go, token_a)
    ok(f"created lobby {lobby_id}")

    # 4. Start the game -> INGAME
    started = await start_game(go_ws_a, lobby_id, drain_queue)
    ok("START_GAME acknowledged" if started else "START_GAME sent")
    await asyncio.sleep(1.0)

    await asyncio.sleep(1.0)

    # 5. Register livestream (user A is in the INGAME lobby)
    reg = await register_livestream(args.go, token_a)
    stream_url = reg.get("url")
    assert stream_url, f"register returned no url: {reg}"
    ok(f"livestream registered, stream url={stream_url}")

    # 5b. List livestreams shows our lobby
    livestreams = await list_livestreams(args.go, token_a)
    found = [ls for ls in livestreams if str(ls.get("lobby_id")) == str(lobby_id)]
    ok(f"livestreams list contains lobby {lobby_id}" if found else
       f"livestreams list does NOT contain {lobby_id}: {livestreams}")

    # 6. Streamer connects to relay
    streamer_ws = await stream_replay(rewrite_relay_host(stream_url, relay_host))

    # 7. Second user (observer) logs in + observes
    login_b = await check_login(args.go)
    token_b = login_b["session_token"]
    ok(f"CheckLogin user B -> user_id={login_b['user_id']}")

    watch_url = await observe(args.go, token_b, lobby_id)
    ok(f"observe returned watch url={watch_url}")

    # 8. Observer connects to relay
    observer_ws = await watch_replay(rewrite_relay_host(watch_url, relay_host))

    # 9. End the stream, cleanup
    await streamer_ws.send(pack_frame(MSG_END, b""))
    await asyncio.sleep(0.3)
    await streamer_ws.close()
    await observer_ws.close()
    drain_stop.set()
    await drain_task
    await go_ws_a.close()

    print("\n" + "=" * 60)
    print(f"RESULTS: {PASS} passed, {FAIL} failed")
    print("=" * 60)
    return FAIL == 0


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
