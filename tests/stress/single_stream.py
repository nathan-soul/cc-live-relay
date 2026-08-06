#!/usr/bin/env python3
"""
Spawn ONE livestream and keep it alive for --duration seconds, pushing replay data.

Minimal path (no observers): CheckLogin -> WS -> lobby -> START_GAME -> /livestreams/register
-> connect relay /stream -> push HEADER + BODY chunks until the duration elapses.

Useful to check whether a single long-lived stream survives on a deployment (the relay
/health should show 1 active game with growing body bytes for the whole run).

Usage:
  GO=https://batty.youbantoo.club/go python tests/stress/single_stream.py --duration 60
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
    body = {"name": "single", "map_name": "single map", "map_path": "single\\single.map",
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
    return lobby_id  # best effort


async def register_livestream(token):
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{GO}/env/prod/contract/1/Livestreams/register",
                          headers={'Authorization': f"Bearer {token}"}) as r:
            d = json.loads(await r.text())
            return r.status, d


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=60.0)
    ap.add_argument("--chunk-interval", type=float, default=0.25)
    args = ap.parse_args()

    print(f"== spawning 1 livestream for {args.duration:.0f}s (GO={GO}) ==")

    host = await login()
    print(f"login ok user_id={host['user_id']} ws_uri={host['ws_uri']}")
    ws = await open_go_ws(host["ws_uri"], host["session_token"])
    q = asyncio.Queue(); st = asyncio.Event()
    dt = asyncio.create_task(drain_go(ws, q, st))
    await asyncio.sleep(0.3)

    lobby_id = await create_ingame_lobby(host["session_token"], ws, q)
    print(f"lobby created + INGAME: {lobby_id}")

    status, reg = await register_livestream(host["session_token"])
    if status != 200 or not reg.get("url"):
        print(f"register FAILED: {status} {reg}")
        return 1
    stream_url = reg["url"]
    print(f"register ok stream_url={stream_url}")

    # connect the streamer and keep pushing until the deadline
    end_time = time.monotonic() + args.duration
    streamer = await websockets.connect(stream_url, open_timeout=10, ping_interval=20)
    await streamer.send(pack(MSG_REGISTER, json.dumps({
        "lobbyid": str(lobby_id), "player_name": "single", "can_stream": True,
        "is_host": True}).encode()))
    raw = await asyncio.wait_for(streamer.recv(), timeout=10)
    t, pl = unpack(raw)
    if t != MSG_ROLE:
        print(f"streamer ROLE FAIL type={t}")
        return 1
    print("streamer connected, ROLE ok; pushing BODY...")
    await streamer.send(pack(MSG_HEADER, b"SINGLE_HEADER"))

    offset = 0
    elapsed = 0.0
    while time.monotonic() < end_time:
        await streamer.send(pack(MSG_BODY, struct.pack('<Q', offset) + b"S" * 2048))
        offset += 2048
        elapsed = time.monotonic() - (end_time - args.duration)
        if int(elapsed) % 10 == 0 and int(elapsed) != 0:
            print(f"  t={elapsed:.0f}s body_offset={offset:,}")
        await asyncio.sleep(args.chunk_interval)

    print(f"done: pushed {offset:,} bytes over {args.duration:.0f}s")
    await streamer.send(pack(MSG_END, b""))
    await streamer.close()
    st.set(); await dt
    await ws.close()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
