#!/usr/bin/env python3
"""Quick live check of all-push demotion + re-promotion over a real relay socket."""
import asyncio
import json
import os
import struct
import sys
import websockets

HOST = os.getenv("RELAY_TEST_HOST", "localhost")
PORT = os.getenv("RELAY_TEST_PORT", "8770")
BASE = f"ws://{HOST}:{PORT}"
HTTP = f"http://{HOST}:{PORT}"
KEY = os.getenv("RELAY_TEST_KEY", "test123")

MSG_REGISTER, MSG_HEADER, MSG_PATCH, MSG_BODY, MSG_END, MSG_ROLE, MSG_ERROR = range(7)


def pack(mt, p=b""):
    return bytes([mt]) + struct.pack("<I", len(p)) + p


def unpack(d):
    if len(d) < 5:
        return (None, b"")
    n = struct.unpack("<I", d[1:5])[0]
    return (d[0], d[5:5 + n])


async def mint(lobby, kind):
    import urllib.request
    body = json.dumps({"lobby_id": lobby, "user_id": 1}).encode()
    req = urllib.request.Request(f"{HTTP}/internal/{kind}",
                                 data=body, headers={"X-Relay-Key": KEY,
                                                     "Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        url = json.loads(r.read())["url"]
    # The minted URL carries the public scheme/host (wss://...); rewrite to the local test
    # address since this harness talks to a plain ws relay.
    from urllib.parse import urlsplit, urlunsplit
    parts = urlsplit(url)
    return urlunsplit(("ws", f"{HOST}:{PORT}", parts.path, parts.query, ""))


async def source_ws(url, name, lobby):
    ws = await websockets.connect(url, max_size=None)
    await ws.send(pack(MSG_REGISTER, json.dumps(
        {"lobbyid": lobby, "player_name": name, "can_stream": True}).encode()))
    role = await ws.recv()
    print(f"  {name}: ROLE={unpack(role)[1].decode()}")
    return ws


async def main():
    lobby = "demote_e2e_001"
    print(f"registering livestream for {lobby}")
    import urllib.request
    req = urllib.request.Request(f"{HTTP}/internal/livestreams",
                                 data=json.dumps({"lobby_id": lobby, "owner_user_id": 1}).encode(),
                                 headers={"X-Relay-Key": KEY, "Content-Type": "application/json"})
    urllib.request.urlopen(req)

    url_a = await mint(lobby, "stream_tokens")
    url_b = await mint(lobby, "stream_tokens")
    a = await source_ws(url_a, "A", lobby)
    b = await source_ws(url_b, "B", lobby)

    # A streams the whole body live; B lags behind with stale chunks -> demote B.
    header = b"H" * 40
    await a.send(pack(MSG_HEADER, header))
    off = 0
    chunk = b"B" * 4096
    for i in range(60):
        await a.send(pack(MSG_BODY, struct.pack("<Q", off) + chunk))
        await b.send(pack(MSG_BODY, struct.pack("<Q", max(0, off - 4096)) + chunk))  # 1 chunk behind
        off += 4096
        try:
            role = await asyncio.wait_for(b.recv(), timeout=2)   # drain any ROLE
        except asyncio.TimeoutError:
            continue
        t, p = unpack(role)
        if t == MSG_ROLE and b'"role":"backup"' in p:
            print("  [PASS] B demoted over the wire while A streams", flush=True)
            break
    else:
        print("  [FAIL] B never demoted", flush=True)
        sys.exit(1)

    # A disconnects (last active pusher) -> B should be re-promoted with takeover + body_offset.
    await a.close()
    for _ in range(3):
        try:
            role = await asyncio.wait_for(b.recv(), timeout=3)
        except asyncio.TimeoutError:
            print("  [FAIL] no takeover ROLE after A left")
            sys.exit(1)
        t, p = unpack(role)
        if t == MSG_ROLE:
            text = p.decode()
            print(f"  B: ROLE={text}")
            if "takeover" in text and f'"body_offset":{off}' in text:
                print("  [PASS] B re-promoted with takeover + correct body_offset")
                break
    else:
        print("  [FAIL] takeover ROLE missing body_offset")
        sys.exit(1)

    # B now streams from body_offset; relay must accept it contiguously.
    await b.send(pack(MSG_BODY, struct.pack("<Q", off) + b"C" * 4096))
    await asyncio.sleep(0.5)
    print("  B resumed streaming from the takeover offset")
    print("ALL WIRE CHECKS PASSED")


asyncio.run(main())
