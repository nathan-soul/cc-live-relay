#!/usr/bin/env python3
"""
Spawn ONE livestream, keep it alive for --duration seconds, and monitor it end-to-end.

Every --monitor-interval seconds it prints:
  - relay /health  (active_games, total_observers, total_body_bytes)
  - GO /livestreams listing (does the lobby appear? observer_count, is_live)
  - the relay /stream connection's own status (connected, bytes pushed)

This tells us WHERE a stream disappears: relay session missing (relay side), or GO not
listing it (GO side), or observer count reporting broken.

Usage:
  GO=https://batty.youbantoo.club/go RELAY_HTTP=https://batty.youbantoo.club/relay \
    python tests/stress/monitor_stream.py --duration 60
"""
import argparse
import asyncio
import json
import os
import struct
import sys
import time

import aiohttp
import websockets

GO = os.getenv("GO", "http://localhost:8080")
RELAY_HTTP = os.getenv("RELAY_HTTP", "http://localhost:8765")

MSG_REGISTER = 0
MSG_HEADER = 1
MSG_BODY = 3
MSG_END = 4
MSG_ROLE = 5
START_GAME = 13


def pack(mt: int, p: bytes = b"") -> bytes:
    return bytes([mt]) + struct.pack('<I', len(p)) + p


def unpack(d: bytes):
    if len(d) < 5:
        return (None, b"")
    return (d[0], d[5:5 + struct.unpack("<I", d[1:5])[0]])


async def login():
    body = {'code': 'ILOVECODE', 'client_id': 'gen_online_30hz',
            'reserved_0': '', 'reserved_1': '', 'reserved_2': '', 'exe_crc': '0', 'ini_crc': '0'}
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{GO}/env/prod/contract/1/CheckLogin", json=body) as r:
            if r.status != 200:
                raise RuntimeError(f"CheckLogin failed {r.status}")
            d = json.loads(await r.text())
            if d.get("result") != 1:
                raise RuntimeError(f"CheckLogin result != 1: {d}")
            return d


async def open_go_ws(ws_uri, token):
    headers = {'Authorization': f"Bearer {token}", 'is-reconnect': 'false',
               'client_id': 'gen_online_30hz'}
    return await websockets.connect(ws_uri, additional_headers=headers,
                                    open_timeout=10, ping_interval=None)


async def drain_go(ws, q, stop):
    try:
        while not stop.is_set():
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except Exception:
                await q.put(None)
                return
            try:
                await q.put(json.loads(raw))
            except Exception:
                pass
    except Exception:
        pass


async def create_ingame_lobby(token, ws, q):
    body = {"name": "monitor", "map_name": "monitor map", "map_path": "monitor\\monitor.map",
            "map_official": False, "max_players": 8, "preferred_port": 1234,
            "vanilla_teams": True, "track_stats": False, "starting_cash": 10000,
            "passworded": False, "password": "", "allow_observers": True,
            "max_cam_height": 3000.0, "exe_crc": 0, "ini_crc": 0, "anticheat_id": 0}
    async with aiohttp.ClientSession() as s:
        async with s.put(f"{GO}/env/prod/contract/1/Lobbies", json=body,
                         headers={'Authorization': f"Bearer {token}"}) as r:
            d = json.loads(await r.text())
            if r.status != 200:
                raise RuntimeError(f"create lobby failed {r.status}: {d}")
            lobby_id = d.get("lobby_id")
            if lobby_id is None:
                raise RuntimeError(f"no lobby_id: {d}")

    await ws.send(json.dumps({"msg_id": START_GAME}))
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            m = await asyncio.wait_for(q.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue
        if m is None:
            return None
        if m.get("msg_id") == START_GAME:
            return lobby_id
    return lobby_id


async def register_livestream(token):
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{GO}/env/prod/contract/1/Livestreams/register",
                          headers={'Authorization': f"Bearer {token}"}) as r:
            d = json.loads(await r.text())
            return r.status, d


async def list_livestreams(token):
    async with aiohttp.ClientSession() as s:
        async with s.get(f"{GO}/env/prod/contract/1/Livestreams",
                         headers={'Authorization': f"Bearer {token}"}) as r:
            if r.status != 200:
                return f"(list HTTP {r.status})"
            return json.loads(await r.text()).get("livestreams", [])


async def relay_health():
    async with aiohttp.ClientSession() as s:
        async with s.get(f"{RELAY_HTTP}/health", timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return f"(health HTTP {r.status})"
            return json.loads(await r.text())


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=60.0)
    ap.add_argument("--chunk-interval", type=float, default=0.25)
    ap.add_argument("--monitor-interval", type=float, default=10.0)
    args = ap.parse_args()

    print(f"== 1 livestream for {args.duration:.0f}s; monitoring every {args.monitor_interval:.0f}s ==")
    print(f"   GO={GO}  RELAY={RELAY_HTTP}")

    host = await login()
    token = host["session_token"]
    ws = await open_go_ws(host["ws_uri"], token)
    q = asyncio.Queue(); st = asyncio.Event()
    dt = asyncio.create_task(drain_go(ws, q, st))
    await asyncio.sleep(0.3)

    lobby_id = await create_ingame_lobby(token, ws, q)
    print(f"lobby {lobby_id} INGAME")

    status, reg = await register_livestream(token)
    if status != 200 or not reg.get("url"):
        print(f"register FAILED: {status} {reg}")
        return 1
    stream_url = reg["url"]
    print(f"registered, stream_url={stream_url}")

    end_time = time.monotonic() + args.duration
    streamer = await websockets.connect(stream_url, open_timeout=10, ping_interval=20)
    await streamer.send(pack(MSG_REGISTER, json.dumps({
        "lobbyid": str(lobby_id), "player_name": "monitor", "can_stream": True,
        "is_host": True}).encode()))
    raw = await asyncio.wait_for(streamer.recv(), timeout=10)
    t, pl = unpack(raw)
    if t != MSG_ROLE:
        print(f"streamer ROLE FAIL type={t}")
        return 1
    print("streamer connected (ROLE ok)\n")
    await streamer.send(pack(MSG_HEADER, b"MONITOR_HEADER"))

    offset = 0
    next_monitor = time.monotonic()
    while time.monotonic() < end_time:
        try:
            await streamer.send(pack(MSG_BODY, struct.pack('<Q', offset) + b"M" * 2048))
            offset += 2048
        except Exception as e:
            print(f"STREAMER SEND FAILED: {type(e).__name__}: {e}")
            break

        now = time.monotonic()
        if now >= next_monitor:
            try:
                h = await relay_health()
            except Exception as e:
                h = f"(relay health error {type(e).__name__}: {e})"
            try:
                lst = await list_livestreams(token)
                mine = [x for x in lst if str(x.get("lobby_id")) == str(lobby_id)]
            except Exception as e:
                mine = f"(list error {type(e).__name__}: {e})"
            print(f"t={now - (end_time - args.duration):6.1f}s  "
                  f"pushed={offset:,}B  relay_health={h}  go_list_has_mine={json.dumps(mine, ensure_ascii=True)}")
            next_monitor = now + args.monitor_interval

        await asyncio.sleep(args.chunk_interval)

    print(f"\ndone: pushed {offset:,} bytes over {args.duration:.0f}s")
    try:
        print(f"final relay_health={await relay_health()}")
    except Exception as e:
        print(f"final relay health error: {type(e).__name__}: {e}")
    await streamer.send(pack(MSG_END, b""))
    await streamer.close()
    st.set(); await dt
    await ws.close()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
