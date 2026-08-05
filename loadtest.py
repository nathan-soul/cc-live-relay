#!/usr/bin/env python3
"""
Quick load test for cc-live-relay against a live deployment.

Simulates N concurrent games, each with 1 streamer source sending BODY chunks at a
steady rate, and M observers watching that game. Measures connect latency, catch-up
latency, live-chunk delivery latency, and error/drop counts.

Usage:
    python loadtest.py --host batty.youbantoo.club --games 5 --observers-per-game 20 \
        --duration 30 --chunk-interval 0.5 --chunk-size 4096
"""
import argparse
import asyncio
import json
import statistics
import struct
import time
import uuid

import websockets

MSG_REGISTER = 0
MSG_HEADER   = 1
MSG_PATCH    = 2
MSG_BODY     = 3
MSG_END      = 4
MSG_ROLE     = 5
MSG_ERROR    = 6


def pack_frame(msg_type: int, payload: bytes = b"") -> bytes:
    return bytes([msg_type]) + struct.pack("<I", len(payload)) + payload


def unpack_frame(data: bytes):
    if len(data) < 5:
        return (None, b"")
    msg_type = data[0]
    payload_len = struct.unpack("<I", data[1:5])[0]
    if len(data) < 5 + payload_len:
        return (None, b"")
    return (msg_type, data[5:5 + payload_len])


class Metrics:
    def __init__(self):
        self.connect_times = []       # ws handshake -> ROLE/first frame
        self.catchup_times = []       # observer connect -> first BODY received
        self.chunk_latencies = []     # send timestamp embedded -> receive timestamp
        self.errors = []
        self.streamer_connects = 0
        self.observer_connects = 0
        self.observer_connect_fails = 0
        self.bytes_sent = 0
        self.bytes_received = 0

    def summary(self):
        def stats(name, values):
            if not values:
                return f"{name}: n=0"
            v = sorted(values)
            return (f"{name}: n={len(v)} avg={statistics.mean(v)*1000:.1f}ms "
                    f"p50={v[len(v)//2]*1000:.1f}ms p95={v[int(len(v)*0.95)]*1000:.1f}ms "
                    f"max={v[-1]*1000:.1f}ms")

        lines = [
            f"streamer connects: {self.streamer_connects}",
            f"observer connects: {self.observer_connects} (failed: {self.observer_connect_fails})",
            stats("connect latency", self.connect_times),
            stats("catchup latency (connect->first body)", self.catchup_times),
            stats("live chunk latency (send->recv)", self.chunk_latencies),
            f"bytes sent: {self.bytes_sent:,}  bytes received: {self.bytes_received:,}",
            f"errors: {len(self.errors)}",
        ]
        if self.errors:
            lines.append("first 10 errors:")
            for e in self.errors[:10]:
                lines.append(f"  - {e}")
        return "\n".join(lines)


async def run_streamer(base: str, lobby_id: str, duration: float, chunk_interval: float,
                        chunk_size: int, metrics: Metrics, stop_event: asyncio.Event,
                        source_index: int = 0, is_primary: bool = True):
    """One /register source connection for a game.

    Real GO lobbies have up to 8 players; any with can_stream=True is a source, and they all
    push the same replay bytes (the relay dedups by offset+content, see apply_body). To model
    that without every source racing to be first at each offset, only the primary source (index
    0) actually advances the body; secondary sources connect, register, and hold the socket open
    with periodic no-op traffic — same connection-count load, without racing writes that would
    make offset bookkeeping in this synthetic client meaningless.
    """
    url = f"{base}/register"
    t0 = time.monotonic()
    try:
        async with websockets.connect(url, open_timeout=10, ping_interval=20) as ws:
            reg = json.dumps({
                "lobbyid": lobby_id,
                "player_name": f"loadtest_streamer_{lobby_id}_{source_index}",
                "can_stream": True,
                "is_host": is_primary,
                **({"delay_seconds": 0} if is_primary else {}),
                "lobby": {
                    "lobbytype": 0, "region": "loadtest", "rngseed": 1,
                    "owner": 1, "name": f"loadtest {lobby_id}",
                    "mapname": "loadtest.map", "mappath": "loadtest.map",
                    "members": [{"userid": 1, "displayname": "loadtest"}],
                } if is_primary else None,
            })
            await ws.send(pack_frame(MSG_REGISTER, reg.encode()))
            raw = await asyncio.wait_for(ws.recv(), timeout=10)
            msg_type, payload = unpack_frame(raw)
            if msg_type != MSG_ROLE:
                metrics.errors.append(f"streamer {lobby_id}#{source_index}: expected ROLE, got {msg_type}")
                return
            metrics.connect_times.append(time.monotonic() - t0)
            metrics.streamer_connects += 1

            end_time = time.monotonic() + duration

            if not is_primary:
                # Secondary source: registered as a real source (holds a slot in
                # session.sources, counted in /games "sources"), but never writes body data —
                # avoids racing the primary's offset bookkeeping in this synthetic client.
                while time.monotonic() < end_time and not stop_event.is_set():
                    await asyncio.sleep(1.0)
                return

            await ws.send(pack_frame(MSG_HEADER, f"HEADER_{lobby_id}".encode()))
            metrics.bytes_sent += len(f"HEADER_{lobby_id}")

            offset = 0
            while time.monotonic() < end_time and not stop_event.is_set():
                # Embed send timestamp as first 8 bytes of chunk payload (after the 8B offset
                # header the protocol already uses) so observers can compute delivery latency.
                send_ts = struct.pack("<d", time.time())
                filler = b"\x42" * max(0, chunk_size - len(send_ts))
                data = send_ts + filler
                payload = struct.pack("<Q", offset) + data
                await ws.send(pack_frame(MSG_BODY, payload))
                metrics.bytes_sent += len(data)
                offset += len(data)
                await asyncio.sleep(chunk_interval)

            await ws.send(pack_frame(MSG_END, b""))
    except Exception as e:
        metrics.errors.append(f"streamer {lobby_id}#{source_index}: {type(e).__name__}: {e}")


async def run_observer(base: str, lobby_id: str, duration: float, metrics: Metrics,
                        stop_event: asyncio.Event):
    url = f"{base}/watch/{lobby_id}"
    t0 = time.monotonic()
    got_first_body = False
    try:
        async with websockets.connect(url, open_timeout=10, ping_interval=20) as ws:
            metrics.observer_connects += 1
            end_time = time.monotonic() + duration + 5
            while time.monotonic() < end_time and not stop_event.is_set():
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                except asyncio.TimeoutError:
                    continue
                if not isinstance(raw, bytes):
                    continue
                msg_type, payload = unpack_frame(raw)
                if msg_type == MSG_BODY:
                    if not got_first_body:
                        metrics.catchup_times.append(time.monotonic() - t0)
                        got_first_body = True
                    metrics.bytes_received += len(payload)
                    # payload = [8B file offset][8B send_ts double][filler]
                    if len(payload) >= 16:
                        try:
                            send_ts = struct.unpack("<d", payload[8:16])[0]
                            latency = time.time() - send_ts
                            if 0 <= latency < 300:
                                metrics.chunk_latencies.append(latency)
                        except struct.error:
                            pass
                elif msg_type == MSG_ERROR:
                    metrics.errors.append(f"observer {lobby_id}: ERROR frame: {payload!r}")
                    return
                elif msg_type == MSG_END:
                    return
    except Exception as e:
        metrics.observer_connect_fails += 1
        metrics.errors.append(f"observer {lobby_id}: {type(e).__name__}: {e}")


async def check_http(base_http: str, metrics: Metrics):
    import aiohttp
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{base_http}/health", timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
                print(f"[health before] {data}")
    except Exception as e:
        metrics.errors.append(f"health check: {type(e).__name__}: {e}")


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True, help="host[:port], no scheme")
    ap.add_argument("--tls", action="store_true", default=True)
    ap.add_argument("--no-tls", dest="tls", action="store_false")
    ap.add_argument("--games", type=int, default=5)
    ap.add_argument("--streamers-per-game", type=int, default=1,
                     help="sources per game (GO lobbies hold up to 8 players, any of which "
                          "may be can_stream=True)")
    ap.add_argument("--observers-per-game", type=int, default=10)
    ap.add_argument("--duration", type=float, default=20.0)
    ap.add_argument("--chunk-interval", type=float, default=0.5)
    ap.add_argument("--chunk-size", type=int, default=2048)
    ap.add_argument("--observer-join-stagger", type=float, default=0.05,
                     help="seconds between each observer's connect within a game")
    args = ap.parse_args()

    ws_scheme = "wss" if args.tls else "ws"
    http_scheme = "https" if args.tls else "http"
    base_ws = f"{ws_scheme}://{args.host}"
    base_http = f"{http_scheme}://{args.host}"

    metrics = Metrics()
    stop_event = asyncio.Event()

    total_streamers = args.games * args.streamers_per_game
    print(f"Target: {base_ws}  |  games={args.games} streamers/game={args.streamers_per_game} "
          f"total_streamers={total_streamers} observers/game={args.observers_per_game} "
          f"total_observers={args.games * args.observers_per_game} duration={args.duration}s")

    await check_http(base_http, metrics)

    lobby_ids = [f"loadtest_{uuid.uuid4().hex[:8]}" for _ in range(args.games)]

    tasks = []
    for lobby_id in lobby_ids:
        for src_idx in range(args.streamers_per_game):
            tasks.append(asyncio.create_task(
                run_streamer(base_ws, lobby_id, args.duration, args.chunk_interval,
                             args.chunk_size, metrics, stop_event,
                             source_index=src_idx, is_primary=(src_idx == 0))))

    # Let streamers register + send a header before observers pile on.
    await asyncio.sleep(1.5)

    for lobby_id in lobby_ids:
        for i in range(args.observers_per_game):
            tasks.append(asyncio.create_task(
                run_observer(base_ws, lobby_id, args.duration, metrics, stop_event)))
            await asyncio.sleep(args.observer_join_stagger)

    start = time.monotonic()
    await asyncio.gather(*tasks, return_exceptions=True)
    elapsed = time.monotonic() - start

    print(f"\nCompleted in {elapsed:.1f}s\n")
    print(metrics.summary())

    await check_http(base_http, metrics)


if __name__ == "__main__":
    asyncio.run(main())
