#!/usr/bin/env python3
"""
GO-scale stress test modeled on a real Generals Online evening:
  1000 players in 226 matches (~5 players/game), 250 people in menus scattering over
  livestreams. One marquee match (popular player vs rival) draws 40% of the viewers.

Profile:
  - 226 games, each with 1 streamer (the host's token via GO /livestreams/register)
  - The marquee game (game 0) gets a 2nd streamer (the rival) via the relay internal mint
  - 250 observers total: 100 (40%) on the marquee game, 150 scattered round-robin over the
    other 225 games
  - All observers: real JWT login -> POST /observe -> watch ticket -> WS /watch

Usage: python tests/stress/stress_226.py [--games 226] [--observers 250] [--marquee-pct 40] [--duration 20]
"""
import argparse
import asyncio
import json
import statistics
import struct
import time

import aiohttp
import websockets

GO = "http://localhost:8080"
RELAY_WS = "ws://localhost:8765"
RELAY_HTTP = "http://localhost:8765"
RELAY_KEY = "test123"

MSG_REGISTER = 0
MSG_HEADER = 1
MSG_BODY = 3
MSG_END = 4
MSG_ROLE = 5
MSG_ERROR = 6
START_GAME = 13


def pack(mt: int, p: bytes = b"") -> bytes:
    return bytes([mt]) + struct.pack('<I', len(p)) + p


def unpack(d: bytes):
    if len(d) < 5:
        return (None, b"")
    return (d[0], d[5:5 + struct.unpack("<I", d[1:5])[0]])


class Metrics:
    def __init__(self):
        self.streamer_ok = 0
        self.streamer_fail = 0
        self.observer_ok = 0
        self.observer_fail = 0
        self.observer_headers = 0
        self.observer_bodies = 0
        self.reg_fail = 0
        self.observe_fail = 0
        self.chunk_lat = []
        self.bytes_received = 0
        self.errors = []

    def summary(self):
        lat = sorted(self.chunk_lat)
        lat_line = "n/a"
        if lat:
            lat_line = (f"n={len(lat)} avg={statistics.mean(lat)*1000:.1f}ms "
                        f"p50={lat[len(lat)//2]*1000:.1f}ms p95={lat[int(len(lat)*0.95)]*1000:.1f}ms "
                        f"max={lat[-1]*1000:.1f}ms")
        lines = [
            f"streamers connected: {self.streamer_ok} (failed {self.streamer_fail})",
            f"observers connected: {self.observer_ok} (failed {self.observer_fail})",
            f"observer HEADER received: {self.observer_headers}, BODY received: {self.observer_bodies}",
            f"register failures: {self.reg_fail}, observe failures: {self.observe_fail}",
            f"live chunk latency: {lat_line}",
            f"bytes received by observers: {self.bytes_received:,}",
            f"errors: {len(self.errors)}",
        ]
        if self.errors:
            lines.append("first 12 errors:")
            for e in self.errors[:12]:
                lines.append(f"  - {e}")
        return "\n".join(lines)


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
    """Host creates a lobby and starts the game. Returns lobby_id (or None)."""
    body = {"name": "stress", "map_name": "stress map", "map_path": "stress\\stress.map",
            "map_official": False, "max_players": 5, "preferred_port": 1234,
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


async def relay_internal_stream_token(lobby_id, user_id):
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{RELAY_HTTP}/internal/stream_tokens",
                          json={"lobby_id": str(lobby_id), "user_id": user_id},
                          headers={"X-Relay-Key": RELAY_KEY}) as r:
            d = json.loads(await r.text())
            if r.status != 200:
                raise RuntimeError(f"internal stream token failed {r.status}: {d}")
            return d["url"]


async def observe(token, lobby_id):
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{GO}/env/prod/contract/1/Livestreams/observe/{lobby_id}",
                          headers={'Authorization': f"Bearer {token}"}) as r:
            d = json.loads(await r.text())
            return r.status, d


async def run_streamer(url, chunk_interval, duration, metrics, stop):
    end_time = time.monotonic() + duration
    try:
        ws = await websockets.connect(url, open_timeout=10, ping_interval=20)
        await ws.send(pack(MSG_REGISTER, json.dumps({
            "lobbyid": "x", "player_name": "stress", "can_stream": True, "is_host": True}).encode()))
        raw = await asyncio.wait_for(ws.recv(), timeout=10)
        t, pl = unpack(raw)
        if t != MSG_ROLE:
            metrics.streamer_fail += 1
            metrics.errors.append(f"streamer ROLE fail: type={t}")
            await ws.close()
            return
        metrics.streamer_ok += 1
        await ws.send(pack(MSG_HEADER, b"STRESS_HEADER"))
        offset = 0
        while time.monotonic() < end_time and not stop.is_set():
            send_ts = struct.pack("<d", time.time())
            payload = send_ts + b"S" * (2048 - 8)
            await ws.send(pack(MSG_BODY, struct.pack('<Q', offset) + payload))
            offset += len(payload)
            await asyncio.sleep(chunk_interval)
        await ws.send(pack(MSG_END, b""))
        await ws.close()
    except Exception as e:
        metrics.streamer_fail += 1
        metrics.errors.append(f"streamer: {type(e).__name__}: {e}")


async def run_observer(url, metrics, watch_s, stop):
    t0 = time.monotonic()
    try:
        ws = await websockets.connect(url, open_timeout=10, ping_interval=20)
        metrics.observer_ok += 1
        got_header = got_body = False
        end = time.monotonic() + watch_s
        while time.monotonic() < end and not stop.is_set():
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=3.0)
            except asyncio.TimeoutError:
                continue
            if not isinstance(raw, bytes):
                continue
            t, pl = unpack(raw)
            if t == MSG_HEADER:
                got_header = True
            elif t == MSG_BODY:
                got_body = True
                metrics.bytes_received += len(pl)
                if len(pl) >= 16:
                    try:
                        send_ts = struct.unpack("<d", pl[8:16])[0]
                        lat = time.time() - send_ts
                        if 0 <= lat < 300:
                            metrics.chunk_lat.append(lat)
                    except struct.error:
                        pass
            if got_header and got_body and time.monotonic() - t0 > 1.0:
                break
        if got_header:
            metrics.observer_headers += 1
        if got_body:
            metrics.observer_bodies += 1
        await ws.close()
    except Exception as e:
        metrics.observer_fail += 1
        metrics.errors.append(f"observer: {type(e).__name__}: {e}")


async def observer_flow(args, metrics, stop, lobby_id):
    try:
        user = await login()
        status, obs = await observe(user["session_token"], lobby_id)
        if status != 200:
            metrics.observe_fail += 1
            metrics.errors.append(f"observe {lobby_id}: {status} {obs.get('detail')}")
            return
        url = obs.get("url")
        if not url:
            metrics.observe_fail += 1
            metrics.errors.append(f"observe {lobby_id}: no url")
            return
        await run_observer(url, metrics, args.duration, stop)
    except Exception as e:
        metrics.observe_fail += 1
        metrics.errors.append(f"observer flow: {type(e).__name__}: {e}")


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--games", type=int, default=226)
    ap.add_argument("--observers", type=int, default=250)
    ap.add_argument("--marquee-pct", type=float, default=40.0)
    ap.add_argument("--duration", type=float, default=20.0)
    ap.add_argument("--chunk-interval", type=float, default=0.25)
    ap.add_argument("--join-stagger", type=float, default=0.02)
    args = ap.parse_args()

    metrics = Metrics()
    stop = asyncio.Event()
    marquee_viewers = int(args.observers * args.marquee_pct / 100.0)
    scatter_viewers = args.observers - marquee_viewers
    print("=" * 62)
    print(f"GO-SCALE STRESS: {args.games} games, {args.observers} observers "
          f"({marquee_viewers} marquee = {args.marquee_pct}%, {scatter_viewers} scattered), "
          f"duration {args.duration}s")
    print("=" * 62)

    # ── Phase 1: create all lobbies INGAME (serial — the ILOVECODE dev login is not
    # concurrency-safe: each CheckLogin computes the next user_id from live sessions and
    # calls ClearDataFromUser, so parallel logins can wipe each other's sessions) ──
    games = []
    for i in range(args.games):
        try:
            host = await login()
            ws = await open_go_ws(host["ws_uri"], host["session_token"])
            q = asyncio.Queue()
            st = asyncio.Event()
            dt = asyncio.create_task(drain_go(ws, q, st))
            await asyncio.sleep(0.2)
            lobby_id = await create_ingame_lobby(host["session_token"], ws, q)
            if lobby_id is not None:
                games.append({"lobby_id": lobby_id, "host_token": host["session_token"],
                              "ws": ws, "drain_task": dt, "drain_stop": st})
            else:
                metrics.reg_fail += 1
                metrics.errors.append(f"lobby {i} setup: no lobby_id")
                st.set()
                await dt
                await ws.close()
        except Exception as e:
            metrics.reg_fail += 1
            metrics.errors.append(f"lobby {i} setup: {type(e).__name__}: {e}")
    print(f"[phase1] created {len(games)}/{args.games} INGAME lobbies")

    # ── Phase 2: streamer URLs ───────────────────────────────────────────
    # game 0 = marquee (2 streamers: host + rival); others 1 streamer (host)
    streamer_urls = []  # index aligns with games
    for gi, game in enumerate(games):
        urls = []
        try:
            status, reg = await register_livestream(game["host_token"])
            if status == 200 and reg.get("url"):
                urls.append(reg["url"])
            else:
                metrics.reg_fail += 1
                metrics.errors.append(f"register {game['lobby_id']}: {status} {reg.get('detail')}")
            if gi == 0:  # marquee: rival also streams
                rival = await login()
                try:
                    urls.append(await relay_internal_stream_token(game["lobby_id"], rival["user_id"]))
                except Exception as e:
                    metrics.reg_fail += 1
                    metrics.errors.append(f"rival token: {type(e).__name__}: {e}")
        except Exception as e:
            metrics.reg_fail += 1
            metrics.errors.append(f"register loop: {type(e).__name__}: {e}")
        streamer_urls.append(urls)
    print(f"[phase2] streamer urls: {sum(len(u) for u in streamer_urls)} total "
          f"(marquee={len(streamer_urls[0]) if streamer_urls else 0})")

    # ── Phase 3: connect streamers, push ─────────────────────────────────
    streamer_tasks = []
    for urls in streamer_urls:
        for u in urls:
            streamer_tasks.append(asyncio.create_task(
                run_streamer(u, args.chunk_interval, args.duration, metrics, stop)))
    await asyncio.sleep(2.0)

    # ── Phase 4: observers with distribution ─────────────────────────────
    observer_tasks = []
    # marquee game gets marquee_viewers
    if games:
        for _ in range(marquee_viewers):
            observer_tasks.append(asyncio.create_task(
                observer_flow(args, metrics, stop, games[0]["lobby_id"])))
            await asyncio.sleep(args.join_stagger)
    # scatter the rest round-robin over games[1:]
    others = games[1:] if len(games) > 1 else []
    for i in range(scatter_viewers):
        if not others:
            break
        game = others[i % len(others)]
        observer_tasks.append(asyncio.create_task(
            observer_flow(args, metrics, stop, game["lobby_id"])))
        await asyncio.sleep(args.join_stagger)

    start = time.monotonic()
    await asyncio.gather(*streamer_tasks, *observer_tasks, return_exceptions=True)
    elapsed = time.monotonic() - start
    stop.set()

    # teardown host WS
    for g in games:
        g["drain_stop"].set()
    await asyncio.gather(*[g["drain_task"] for g in games], return_exceptions=True)
    await asyncio.gather(*[g["ws"].close() for g in games], return_exceptions=True)

    print(f"\nCompleted in {elapsed:.1f}s\n")
    print(metrics.summary())

    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{RELAY_HTTP}/health") as r:
                print(f"[health] {await r.text()}")
    except Exception as e:
        print(f"[health] {type(e).__name__}: {e}")

    return metrics.observer_fail == 0 and metrics.streamer_fail == 0


if __name__ == "__main__":
    ok_all = asyncio.run(main())
    print("\nRESULT:", "PASS" if ok_all else "FAIL")
    raise SystemExit(0 if ok_all else 1)
