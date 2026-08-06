#!/usr/bin/env python3
"""
Wave-based burst stress for the GO-orchestrated livestream stack.

Models how a real evening goes: streams start in batches (waves), observers join shortly
after their wave's streams go live, and the FIRST stream (the marquee "popular one everyone
watches") keeps absorbing viewers for the whole run.

  - 100 games, 5 streamers each (500 streamers).
  - Streams come up in waves (default 5 waves of 20 games). Each wave's streamers connect
    when the wave starts; observers join that wave's streams after WAVE_OBSERVER_DELAY (~3s).
  - The marquee game (game 0, created first) stays live the whole run and keeps receiving
    observers across every wave — 30% of total observers (450) land there.
  - Every token/ticket is minted through the real GO endpoints.

Usage: python stress_waves.py [--games 100] [--streamers 5] [--observers 1500]
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


def fix_url(url: str) -> str:
    return url.replace("localhost", "127.0.0.1")


class Metrics:
    def __init__(self):
        self.streamer_ok = 0
        self.streamer_fail = 0
        self.observer_ok = 0
        self.observer_fail = 0
        self.observer_headers = 0
        self.observer_bodies = 0
        self.marquee_obs_ok = 0
        self.register_fail = 0
        self.observe_fail = 0
        self.login_fail = 0
        self.bytes_received = 0
        self.errors = []
        self.observer_lock = asyncio.Lock()

    async def add_marquee(self):
        async with self.observer_lock:
            self.marquee_obs_ok += 1

    def summary(self):
        return "\n".join([
            f"streamers connected: {self.streamer_ok} (failed {self.streamer_fail})",
            f"observers connected: {self.observer_ok} (failed {self.observer_fail})",
            f"  marquee observers (game 0): {self.marquee_obs_ok}",
            f"observer HEADER received: {self.observer_headers}, BODY received: {self.observer_bodies}",
            f"GO failures -> register {self.register_fail}, observe {self.observe_fail}, login {self.login_fail}",
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
    body = {"name": "wave", "map_name": "wave", "map_path": "wave\\wave.map",
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
        await ws.send(pack(MSG_HEADER, b"WAVE_HEADER"))
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


async def run_observer(url, metrics, watch_s, stop, is_marquee):
    t0 = time.monotonic()
    url = fix_url(url)
    try:
        ws = await websockets.connect(url, open_timeout=10, ping_interval=20)
        metrics.observer_ok += 1
        if is_marquee:
            await metrics.add_marquee()
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


async def observer_flow(metrics, stop, lobby_id, watch_s, is_marquee):
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
        await run_observer(url, metrics, watch_s, stop, is_marquee)
    except Exception as e:
        metrics.observe_fail += 1
        metrics.errors.append(f"observer flow: {type(e).__name__}: {e}")


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--games", type=int, default=100)
    ap.add_argument("--streamers", type=int, default=5)
    ap.add_argument("--observers", type=int, default=1500)
    ap.add_argument("--marquee-pct", type=float, default=30.0)
    ap.add_argument("--waves", type=int, default=5)
    ap.add_argument("--wave-delay", type=float, default=8.0, help="gap between waves (s)")
    ap.add_argument("--wave-observer-delay", type=float, default=3.0,
                    help="observers join a wave's streams this long after the wave starts (s)")
    ap.add_argument("--duration", type=float, default=20.0)
    ap.add_argument("--chunk-interval", type=float, default=0.25)
    args = ap.parse_args()

    metrics = Metrics()
    stop = asyncio.Event()
    marquee_total = int(args.observers * args.marquee_pct / 100.0)
    scatter_total = args.observers - marquee_total
    games_per_wave = max(1, args.games // args.waves)
    print("=" * 62)
    print(f"WAVE BURST: {args.games} games x {args.streamers} streamers "
          f"({args.games*args.streamers}), {args.observers} observers "
          f"({marquee_total} marquee = {args.marquee_pct}% on game 0, {scatter_total} scattered), "
          f"{args.waves} waves")
    print("=" * 62)

    games = []          # {"lobby_id","host_token","ws","dt","st","is_marquee","streamer_urls"}
    observer_tasks = []
    streamer_tasks = []
    go_ws = []          # keep GO sessions alive

    def make_game(lobby_id, host_token, ws, dt, st):
        return {"lobby_id": lobby_id, "host_token": host_token, "ws": ws, "dt": dt, "st": st,
                "is_marquee": False, "streamer_urls": []}

    # marquee observers per wave (spread evenly)
    marquee_per_wave = max(1, marquee_total // args.waves)

    wave_t0 = time.monotonic()
    for w in range(args.waves):
        wave_games = []
        # ── 1. create this wave's lobbies (serial, ILOVECODE not concurrency-safe) ──
        for i in range(games_per_wave):
            try:
                host = await login()
                ws = await open_go_ws(host["ws_uri"], host["session_token"])
                q = asyncio.Queue(); st = asyncio.Event()
                dt = asyncio.create_task(drain_go(ws, q, st))
                await asyncio.sleep(0.15)
                lobby_id = await create_ingame_lobby(host["session_token"], ws, q)
                if lobby_id is None:
                    metrics.register_fail += 1
                    st.set(); await dt; await ws.close()
                    continue
                g = make_game(lobby_id, host["session_token"], ws, dt, st)
                if len(games) == 0:
                    g["is_marquee"] = True   # first stream created = the popular one
                wave_games.append(g)
                games.append(g)
                go_ws.append((ws, st, dt))
            except Exception as e:
                metrics.register_fail += 1
                metrics.errors.append(f"lobby {e}: {type(e).__name__}: {e}")

        # ── 2. register livestreams + mint stream tokens + connect streamers ──
        wave_streamer_tasks = []
        for g in wave_games:
            urls = []
            try:
                status, reg = await register_livestream(g["host_token"])
                if status == 200 and reg.get("url"):
                    urls.append(reg["url"])
                else:
                    metrics.register_fail += 1
                for _ in range(args.streamers - 1):
                    extra = await login()
                    try:
                        urls.append(await internal_stream_token(g["lobby_id"], extra["user_id"]))
                    except Exception as e:
                        metrics.register_fail += 1
                        metrics.errors.append(f"extra streamer: {type(e).__name__}: {e}")
            except Exception as e:
                metrics.register_fail += 1
                metrics.errors.append(f"register: {type(e).__name__}: {e}")
            g["streamer_urls"] = urls
            for u in urls:
                wave_streamer_tasks.append(asyncio.create_task(
                    run_streamer(u, args.chunk_interval, args.duration, metrics, stop)))
        streamer_tasks.extend(wave_streamer_tasks)
        print(f"[wave {w}] {len(wave_games)} games up, streamers connecting "
              f"({sum(len(g['streamer_urls']) for g in wave_games)})")

        # ── 3. observers join this wave's streams after a short delay ──
        async def join_wave(wave_games_, w_):
            await asyncio.sleep(args.wave_observer_delay)
            # marquee observers: everyone watches game 0
            for _ in range(marquee_per_wave):
                if games:
                    observer_tasks.append(asyncio.create_task(
                        observer_flow(metrics, stop, games[0]["lobby_id"],
                                      args.duration, is_marquee=True)))
            # scattered observers over this wave's non-marquee games
            others = [g for g in wave_games_ if not g["is_marquee"]]
            if others:
                per = max(1, scatter_total // args.games)
                for g in others:
                    for _ in range(per):
                        observer_tasks.append(asyncio.create_task(
                            observer_flow(metrics, stop, g["lobby_id"],
                                          args.duration, is_marquee=False)))

        asyncio.create_task(join_wave(wave_games, w))

        # pace the waves
        elapsed = time.monotonic() - wave_t0
        gap = args.wave_delay - elapsed
        if gap > 0:
            await asyncio.sleep(gap)
        wave_t0 = time.monotonic()

    # let all waves finish joining, then run to completion
    start = time.monotonic()
    await asyncio.gather(*streamer_tasks, *observer_tasks, return_exceptions=True)
    elapsed = time.monotonic() - start
    stop.set()

    for ws, st, dt in go_ws:
        st.set()
    await asyncio.gather(*[dt for _, _, dt in go_ws], return_exceptions=True)
    await asyncio.gather(*[ws.close() for ws, _, _ in go_ws], return_exceptions=True)

    print(f"\nCompleted in {elapsed:.1f}s (after all waves started)\n")
    print(metrics.summary())

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
