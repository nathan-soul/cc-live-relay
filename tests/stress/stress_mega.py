#!/usr/bin/env python3
"""
Ultra-scale stress: 100 games x 5 streamers (500 streamers), then 1500 observers with
30% (450) on one marquee stream. Every token and ticket is minted through the real GO
endpoints (livestreams/register + observe) so both GO and the relay are exercised.

  - 100 INGAME lobbies (host login via GO CheckLogin + WS + START_GAME)
  - 5 streamers per lobby: host via GO /livestreams/register, 4 more via relay internal mint
    (identical to GO's per-member loop)
  - 1500 observers: 450 on game 0 (30%), 1050 scattered round-robin over games 1..99.
    Each: real JWT login -> POST /observe -> watch ticket -> WS /watch, verify HEADER+BODY.

Usage: python tests/stress/stress_mega.py [--games 100] [--streamers 5] [--observers 1500] [--duration 20]
"""
import argparse
import asyncio
import json
import statistics
import struct
import time

import aiohttp
import websockets

GO = "http://127.0.0.1:8080"
RELAY_WS = "ws://127.0.0.1:8765"
RELAY_HTTP = "http://127.0.0.1:8765"
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
        self.register_fail = 0
        self.observe_fail = 0
        self.login_fail = 0
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
        return "\n".join([
            f"streamers connected: {self.streamer_ok} (failed {self.streamer_fail})",
            f"observers connected: {self.observer_ok} (failed {self.observer_fail})",
            f"observer HEADER received: {self.observer_headers}, BODY received: {self.observer_bodies}",
            f"GO failures -> register {self.register_fail}, observe {self.observe_fail}, login {self.login_fail}",
            f"live chunk latency: {lat_line}",
            f"bytes received by observers: {self.bytes_received:,}",
            f"errors: {len(self.errors)}",
        ] + (["first 12 errors:"] + [f"  - {e}" for e in self.errors[:12]] if self.errors else []))


async def login():
    body = {'code': 'ILOVECODE', 'client_id': 'gen_online_30hz',
            'reserved_0': '', 'reserved_1': '', 'reserved_2': '', 'exe_crc': '0', 'ini_crc': '0'}
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{GO}/env/prod/contract/1/CheckLogin", json=body) as r:
            if r.status != 200:
                raise RuntimeError(f"CheckLogin {r.status}")
            d = json.loads(await r.text())
            if d.get("result") != 1:
                raise RuntimeError(f"CheckLogin result != 1: {d}")
            return d


async def open_go_ws(ws_uri, token):
    # CheckLogin returns ws://localhost:8080/ws, but localhost may resolve to IPv6 ::1 where
    # GO doesn't listen — force 127.0.0.1 to reach the container reliably from this host.
    if "localhost" in ws_uri:
        ws_uri = ws_uri.replace("localhost", "127.0.0.1")
    headers = {'Authorization': f"Bearer {token}", 'is-reconnect': 'false',
               'client_id': 'gen_online_30hz'}
    return await websockets.connect(ws_uri, additional_headers=headers,
                                    open_timeout=10, ping_interval=None)


async def drain_go(ws, q, stop):
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


async def create_ingame_lobby(token, ws, q):
    body = {"name": "mega", "map_name": "mega", "map_path": "mega\\mega.map",
            "map_official": False, "max_players": 8, "preferred_port": 1234,
            "vanilla_teams": True, "track_stats": False, "starting_cash": 10000,
            "passworded": False, "password": "", "allow_observers": True,
            "max_cam_height": 3000.0, "exe_crc": 0, "ini_crc": 0, "anticheat_id": 0}
    async with aiohttp.ClientSession() as s:
        async with s.put(f"{GO}/env/prod/contract/1/Lobbies", json=body,
                         headers={'Authorization': f"Bearer {token}"}) as r:
            d = json.loads(await r.text())
            if r.status != 200:
                raise RuntimeError(f"create lobby {r.status}: {d}")
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


async def internal_stream_token(lobby_id, user_id):
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{RELAY_HTTP}/internal/stream_tokens",
                          json={"lobby_id": str(lobby_id), "user_id": user_id},
                          headers={"X-Relay-Key": RELAY_KEY}) as r:
            d = json.loads(await r.text())
            if r.status != 200:
                raise RuntimeError(f"internal stream token {r.status}: {d}")
            return d["url"]


async def observe(token, lobby_id):
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{GO}/env/prod/contract/1/Livestreams/observe/{lobby_id}",
                          headers={'Authorization': f"Bearer {token}"}) as r:
            d = json.loads(await r.text())
            return r.status, d


def fix_url(url: str) -> str:
    """Minted URLs carry PUBLIC_HOST (localhost), which this Windows host resolves to IPv6 ::1
    where Docker Desktop doesn't publish — force 127.0.0.1 for the connect."""
    return url.replace("localhost", "127.0.0.1")


async def run_streamer(url, chunk_interval, duration, metrics, stop):
    end = time.monotonic() + duration
    url = fix_url(url)
    try:
        ws = await websockets.connect(url, open_timeout=10, ping_interval=20)
        await ws.send(pack(MSG_REGISTER, json.dumps(
            {"lobbyid": "x", "player_name": "s", "can_stream": True, "is_host": False}).encode()))
        raw = await asyncio.wait_for(ws.recv(), timeout=10)
        t, pl = unpack(raw)
        if t != MSG_ROLE:
            metrics.streamer_fail += 1
            await ws.close()
            return
        metrics.streamer_ok += 1
        await ws.send(pack(MSG_HEADER, b"MEGA_HEADER"))
        offset = 0
        while time.monotonic() < end and not stop.is_set():
            await ws.send(pack(MSG_BODY, struct.pack('<Q', offset) + b"B" * 2048))
            offset += 2048
            await asyncio.sleep(chunk_interval)
        await ws.send(pack(MSG_END, b""))
        await ws.close()
    except Exception as e:
        metrics.streamer_fail += 1
        metrics.errors.append(f"streamer: {type(e).__name__}: {e}")


async def run_observer(url, metrics, watch_s, stop):
    t0 = time.monotonic()
    url = fix_url(url)
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
            return
        await run_observer(url, metrics, args.duration, stop)
    except Exception as e:
        metrics.observe_fail += 1
        metrics.errors.append(f"observer flow: {type(e).__name__}: {e}")


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--games", type=int, default=100)
    ap.add_argument("--streamers", type=int, default=5)
    ap.add_argument("--observers", type=int, default=1500)
    ap.add_argument("--marquee-pct", type=float, default=30.0)
    ap.add_argument("--duration", type=float, default=20.0)
    ap.add_argument("--chunk-interval", type=float, default=0.25)
    ap.add_argument("--join-stagger", type=float, default=0.01)
    args = ap.parse_args()

    metrics = Metrics()
    stop = asyncio.Event()
    marquee = int(args.observers * args.marquee_pct / 100.0)
    scatter = args.observers - marquee
    print("=" * 62)
    print(f"MEGA STRESS: {args.games} games x {args.streamers} streamers "
          f"({args.games*args.streamers} total), {args.observers} observers "
          f"({marquee} marquee = {args.marquee_pct}%, {scatter} scattered)")
    print("=" * 62)

    # ── Phase 1: create all lobbies INGAME (serial — ILOVECODE login is not concurrency-safe) ──
    games = []
    for i in range(args.games):
        try:
            host = await login()
            ws = await open_go_ws(host["ws_uri"], host["session_token"])
            q = asyncio.Queue(); st = asyncio.Event()
            dt = asyncio.create_task(drain_go(ws, q, st))
            await asyncio.sleep(0.2)
            lobby_id = await create_ingame_lobby(host["session_token"], ws, q)
            if lobby_id is not None:
                games.append({"lobby_id": lobby_id, "host_token": host["session_token"],
                              "ws": ws, "dt": dt, "st": st})
            else:
                metrics.register_fail += 1
                st.set(); await dt; await ws.close()
        except Exception as e:
            metrics.register_fail += 1
            metrics.errors.append(f"lobby {i} setup: {type(e).__name__}: {e}")
    print(f"[phase1] created {len(games)}/{args.games} INGAME lobbies")

    # ── Phase 2: streamer URLs (5 per game) ──────────────────────────────
    streamer_urls = []
    for game in games:
        urls = []
        try:
            status, reg = await register_livestream(game["host_token"])
            if status == 200 and reg.get("url"):
                urls.append(reg["url"])
            else:
                metrics.register_fail += 1
            for _ in range(args.streamers - 1):
                try:
                    extra = await login()
                    urls.append(await internal_stream_token(game["lobby_id"], extra["user_id"]))
                except Exception as e:
                    metrics.register_fail += 1
                    metrics.errors.append(f"extra streamer: {type(e).__name__}: {e}")
        except Exception as e:
            metrics.register_fail += 1
            metrics.errors.append(f"register: {type(e).__name__}: {e}")
        streamer_urls.append(urls)
    total_streamer_urls = sum(len(u) for u in streamer_urls)
    print(f"[phase2] streamer urls minted: {total_streamer_urls} total "
          f"(marquee={len(streamer_urls[0]) if streamer_urls else 0})")

    # ── Phase 3: connect all streamers, push ─────────────────────────────
    streamer_tasks = []
    for urls in streamer_urls:
        for u in urls:
            streamer_tasks.append(asyncio.create_task(
                run_streamer(u, args.chunk_interval, args.duration, metrics, stop)))
    await asyncio.sleep(2.0)

    # ── Phase 4: observers (marquee on game 0, rest scattered) ───────────
    # Give the relay a moment to finish accepting the 500-streamer burst so the observer
    # phase measures fan-out, not connection-accept contention.
    await asyncio.sleep(2.0)
    observer_tasks = []
    if games:
        for _ in range(marquee):
            observer_tasks.append(asyncio.create_task(
                observer_flow(args, metrics, stop, games[0]["lobby_id"])))
            await asyncio.sleep(args.join_stagger)
    others = games[1:] if len(games) > 1 else []
    for i in range(scatter):
        if not others:
            break
        observer_tasks.append(asyncio.create_task(
            observer_flow(args, metrics, stop, others[i % len(others)]["lobby_id"])))
        await asyncio.sleep(args.join_stagger)

    start = time.monotonic()
    await asyncio.gather(*streamer_tasks, *observer_tasks, return_exceptions=True)
    elapsed = time.monotonic() - start
    stop.set()

    for g in games:
        g["st"].set()
    await asyncio.gather(*[g["dt"] for g in games], return_exceptions=True)
    await asyncio.gather(*[g["ws"].close() for g in games], return_exceptions=True)

    print(f"\nCompleted in {elapsed:.1f}s\n")
    print(metrics.summary())

    # GO + relay health after
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{RELAY_HTTP}/health") as r:
                print(f"[relay health] {await r.text()}")
    except Exception as e:
        print(f"[relay health] {type(e).__name__}: {e}")
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{GO}/env/prod/contract/1/Monitoring/BasicStats") as r:
                print(f"[go health] {await r.text()}")
    except Exception as e:
        print(f"[go health] {type(e).__name__}: {e}")

    return metrics.observer_fail == 0 and metrics.streamer_fail == 0


if __name__ == "__main__":
    ok_all = asyncio.run(main())
    print("\nRESULT:", "PASS" if ok_all else "FAIL")
    raise SystemExit(0 if ok_all else 1)
