#!/usr/bin/env python3
"""
Progressive-ramp stress for the GO-orchestrated livestream stack.

Scales up in steps so load ramps rather than bursting: step N adds `games_per_step`
lobbies and `observers_per_step` observers, so the run goes 10 lobbies/100 observers,
then 20/200, 30/300, ... up to `--max-games`/`--max-observers`. Observer joins within a
step are staggered so the added load is spread across the step, not applied at once.

  - Every lobby is created INGAME through GO (CheckLogin + WS + START_GAME), host registers
    the livestream via GO, and the host connects to the relay /stream and pushes replay data.
  - Each new observer does a real CheckLogin -> POST /observe -> watch ticket -> WS /watch.
  - Observers distribute round-robin over the games that exist at their step.

Usage:
  python tests/stress/stress_ramp.py --max-games 100 --max-observers 1000 \
      --steps 10 --duration 60
"""
import argparse
import asyncio
import json
import os
import statistics
import struct
import time

import aiohttp
import websockets

# Endpoints are overridable via env so the same script targets a local test stack
# (defaults) or the batty deployment (e.g. GO=https://batty.youbantoo.club/go
# RELAY_HTTP=https://batty.youbantoo.club/relay RELAY_KEY=<batty INTERNAL_API_KEY>).
GO = os.getenv("GO", "http://localhost:8080")
RELAY_WS = os.getenv("RELAY_WS", "ws://localhost:8765")
RELAY_HTTP = os.getenv("RELAY_HTTP", "http://localhost:8765")
RELAY_KEY = os.getenv("RELAY_KEY", "test123")

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
        self.login_fail = 0
        self.bytes_received = 0
        self.errors = []

    def summary(self):
        return "\n".join([
            f"streamers connected: {self.streamer_ok} (failed {self.streamer_fail})",
            f"observers connected: {self.observer_ok} (failed {self.observer_fail})",
            f"observer HEADER received: {self.observer_headers}, BODY received: {self.observer_bodies}",
            f"GO failures -> register {self.reg_fail}, observe {self.observe_fail}, login {self.login_fail}",
            f"bytes received by observers: {self.bytes_received:,}",
            f"errors: {len(self.errors)}",
        ] + (["first 12 errors:"] + [f"  - {e}" for e in self.errors[:12]] if self.errors else []))


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
    body = {"name": "ramp", "map_name": "ramp map", "map_path": "ramp\\ramp.map",
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
    return lobby_id  # best effort; lobby may still be INGAME


async def register_livestream(token):
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{GO}/env/prod/contract/1/Livestreams/register",
                          headers={'Authorization': f"Bearer {token}"}) as r:
            d = json.loads(await r.text())
            return r.status, d


async def observe(token, lobby_id):
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{GO}/env/prod/contract/1/Livestreams/observe/{lobby_id}",
                          headers={'Authorization': f"Bearer {token}"}) as r:
            d = json.loads(await r.text())
            return r.status, d


async def run_streamer(url, chunk_interval, end_time, metrics, stop):
    """One streamer: push replay data until the global end_time (all streams stay live
    for the whole test so the "live games" count keeps ramping instead of falling)."""
    try:
        ws = await websockets.connect(url, open_timeout=10, ping_interval=20)
        await ws.send(pack(MSG_REGISTER, json.dumps({
            "lobbyid": "x", "player_name": "ramp", "can_stream": True, "is_host": True}).encode()))
        raw = await asyncio.wait_for(ws.recv(), timeout=10)
        t, pl = unpack(raw)
        if t != MSG_ROLE:
            metrics.streamer_fail += 1
            metrics.errors.append(f"streamer ROLE fail: type={t}")
            await ws.close()
            return
        metrics.streamer_ok += 1
        await ws.send(pack(MSG_HEADER, b"RAMP_HEADER"))
        offset = 0
        while time.monotonic() < end_time and not stop.is_set():
            await ws.send(pack(MSG_BODY, struct.pack('<Q', offset) + b"R" * 2048))
            offset += 2048
            await asyncio.sleep(chunk_interval)
        await ws.send(pack(MSG_END, b""))
        await ws.close()
    except Exception as e:
        metrics.streamer_fail += 1
        metrics.errors.append(f"streamer: {type(e).__name__}: {e}")


async def run_observer(url, metrics, end_time, stop):
    """One observer: stay connected and read until the global end_time, so concurrent
    observers accumulate with the ramp instead of each dropping off after 1s."""
    t0 = time.monotonic()
    got_header = got_body = False
    peak_metric = False
    try:
        ws = await websockets.connect(url, open_timeout=10, ping_interval=20)
        metrics.observer_ok += 1
        while time.monotonic() < end_time and not stop.is_set():
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
        if got_header:
            metrics.observer_headers += 1
        if got_body:
            metrics.observer_bodies += 1
        await ws.close()
    except Exception as e:
        metrics.observer_fail += 1
        metrics.errors.append(f"observer: {type(e).__name__}: {e}")


async def observer_flow(metrics, stop, lobby_id, end_time):
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
        await run_observer(url, metrics, end_time, stop)
    except Exception as e:
        metrics.observe_fail += 1
        metrics.errors.append(f"observer flow: {type(e).__name__}: {e}")


async def relay_health():
    async with aiohttp.ClientSession() as s:
        async with s.get(f"{RELAY_HTTP}/health", timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return f"(health HTTP {r.status})"
            return json.loads(await r.text())


async def go_live_count(token):
    async with aiohttp.ClientSession() as s:
        async with s.get(f"{GO}/env/prod/contract/1/Livestreams",
                         headers={'Authorization': f"Bearer {token}"}) as r:
            if r.status != 200:
                return -1
            return len(json.loads(await r.text()).get("livestreams", []))


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-games", type=int, default=100)
    ap.add_argument("--max-observers", type=int, default=1000)
    ap.add_argument("--steps", type=int, default=10, help="how many ramp steps (10 -> 10,20,... max-games)")
    ap.add_argument("--duration", type=float, default=60.0, help="total run time (s); the ramp fills it")
    ap.add_argument("--chunk-interval", type=float, default=0.25)
    ap.add_argument("--monitor-interval", type=float, default=10.0,
                    help="seconds between relay-health / GO-livestreams monitor prints")
    args = ap.parse_args()

    games_per_step = max(1, args.max_games // args.steps)
    obs_per_step = max(1, args.max_observers // args.steps)
    step_duration = args.duration / args.steps
    # Stagger so each step's observers join across the step (not all at once).
    join_stagger = min(0.05, step_duration / obs_per_step)

    metrics = Metrics()
    stop = asyncio.Event()
    # All streamers and observers run until this single deadline, so live games and
    # concurrent observers keep accumulating through the ramp instead of dropping off.
    test_end = time.monotonic() + args.duration
    games = []          # {"lobby_id","host_token","ws","dt","st","streamer_urls"}
    go_ws = []
    streamer_tasks = []
    observer_tasks = []

    # Background monitor: prints relay /health + GO /livestreams count every interval so we
    # see the exact moment games start disappearing (session teardown vs. load).
    mon_token = None
    mon_next = time.monotonic() + args.monitor_interval

    async def monitor_loop():
        nonlocal mon_token, mon_next
        while not stop.is_set():
            try:
                if mon_token is None:
                    try:
                        mon_token = (await login())["session_token"]
                    except Exception:
                        mon_token = "login-failed"
                h = await relay_health()
                go_n = await go_live_count(mon_token) if isinstance(mon_token, str) and mon_token != "login-failed" else -2
                t = time.monotonic() - (test_end - args.duration)
                print(f"  [monitor t={t:5.1f}s] relay={h} go_live_listed={go_n} "
                      f"games={len(games)} obs_tasks={len(observer_tasks)} "
                      f"obs_ok={metrics.observer_ok} obs_fail={metrics.observer_fail}")
            except Exception as e:
                print(f"  [monitor error] {type(e).__name__}: {e}")
            mon_next = time.monotonic() + args.monitor_interval
            # sleep in small slices so stop.set() is noticed promptly
            for _ in range(int(args.monitor_interval / 0.5) or 1):
                await asyncio.sleep(0.5)
                if stop.is_set():
                    return

    monitor_task = asyncio.create_task(monitor_loop())

    print("=" * 62)
    print(f"PROGRESSIVE RAMP: up to {args.max_games} games x 1 streamer, "
          f"{args.max_observers} observers over {args.steps} steps ({step_duration:.1f}s each), "
          f"{args.duration:.0f}s total")
    print("=" * 62)

    target_games = 0
    target_obs = 0
    step_t0 = time.monotonic()
    for step in range(1, args.steps + 1):
        target_games += games_per_step
        target_obs += obs_per_step

        # ── 1. bring up this step's new lobbies ──
        while len(games) < target_games:
            try:
                host = await login()
                ws = await open_go_ws(host["ws_uri"], host["session_token"])
                q = asyncio.Queue(); st = asyncio.Event()
                dt = asyncio.create_task(drain_go(ws, q, st))
                await asyncio.sleep(0.15)
                lobby_id = await create_ingame_lobby(host["session_token"], ws, q)
                if lobby_id is None:
                    metrics.reg_fail += 1
                    st.set(); await dt; await ws.close()
                    continue
                # register livestream (host) -> streamer URL via GO
                urls = []
                status, reg = await register_livestream(host["session_token"])
                if status == 200 and reg.get("url"):
                    urls.append(reg["url"])
                else:
                    metrics.reg_fail += 1
                    metrics.errors.append(f"register {lobby_id}: {status} {reg.get('detail')}")
                games.append({"lobby_id": lobby_id, "host_token": host["session_token"],
                              "ws": ws, "dt": dt, "st": st, "streamer_urls": urls})
                go_ws.append((ws, st, dt))
                for u in urls:
                    streamer_tasks.append(asyncio.create_task(
                        run_streamer(u, args.chunk_interval, test_end, metrics, stop)))
            except Exception as e:
                metrics.reg_fail += 1
                metrics.errors.append(f"lobby setup: {type(e).__name__}: {e}")

        # ── 2. add this step's observers, spread across the step ──
        while len(games) and len(observer_tasks) < target_obs:
            for _ in range(target_obs - len(observer_tasks)):
                game = games[len(observer_tasks) % len(games)]
                observer_tasks.append(asyncio.create_task(
                    observer_flow(metrics, stop, game["lobby_id"], test_end)))
                await asyncio.sleep(join_stagger)

        # ── 3. pace the step ──
        elapsed = time.monotonic() - step_t0
        gap = step_duration - elapsed
        if gap > 0:
            await asyncio.sleep(gap)
        step_t0 = time.monotonic()
        print(f"[step {step}/{args.steps}] games={len(games)}/{target_games} "
              f"observers={len(observer_tasks)}/{target_obs} "
              f"(ok {metrics.observer_ok} fail {metrics.observer_fail})")

    # ── run out the ramp ──
    start = time.monotonic()
    await asyncio.gather(*streamer_tasks, *observer_tasks, return_exceptions=True)
    elapsed = time.monotonic() - start
    stop.set()

    monitor_task.cancel()
    try:
        await monitor_task
    except asyncio.CancelledError:
        pass

    for ws, st, dt in go_ws:
        st.set()
    await asyncio.gather(*[dt for _, _, dt in go_ws], return_exceptions=True)
    await asyncio.gather(*[ws.close() for ws, _, _ in go_ws], return_exceptions=True)

    print(f"\nCompleted in {elapsed:.1f}s (after ramp filled)\n")
    print(metrics.summary())

    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{RELAY_HTTP}/health") as r:
                print(f"[relay health] {await r.text()}")
    except Exception as e:
        print(f"[relay health] {type(e).__name__}: {e}")

    return metrics.observer_fail == 0 and metrics.streamer_fail == 0


if __name__ == "__main__":
    ok_all = asyncio.run(main())
    print("\nRESULT:", "PASS" if ok_all else "FAIL")
    raise SystemExit(0 if ok_all else 1)
