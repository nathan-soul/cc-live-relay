#!/usr/bin/env python3
"""
Unit tests for GameSession delivery semantics. No server required.

Complements test_relay.py (which drives a running relay over the network) by exercising
the observer-join interleavings directly. Those are racy by nature and cannot be forced
reliably over a real socket, but they are exactly where a bug corrupts an observer's
replay file: observers write each BODY chunk at its absolute file offset, so a chunk
delivered out of order leaves a hole, and a chunk delivered twice rewinds the client's
parse cursor.

The invariant both scenarios check is the same: across catch-up and live broadcast, every
body byte reaches the observer exactly once, in order, with no gap and no overlap.

Run: python test_session_unit.py
"""
import asyncio
import struct
import sys

import server
from server import GameSession, MSG_ROLE, MSG_HEADER, MSG_BODY

PASS = 0
FAIL = 0

HEADER_LEN = 40
INITIAL_BODY = 100
LIVE_CHUNK = 20
DELAY_SECONDS = 42


class FakeWS:
    """Records the frames sent to it, and yields so ordering bugs can surface."""

    def __init__(self):
        self.frames = []

    async def send_bytes(self, data: bytes):
        n = struct.unpack("<I", data[1:5])[0]
        self.frames.append((data[0], data[5:5 + n]))
        await asyncio.sleep(0)


def check(label: str, condition: bool, detail: str = ""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS  {label}")
    else:
        FAIL += 1
        print(f"  FAIL  {label}  {detail}")


def body_spans(ws):
    """File-offset spans of BODY frames, in delivery order."""
    spans = []
    for msg_type, payload in ws.frames:
        if msg_type == MSG_BODY:
            offset = struct.unpack("<Q", payload[:8])[0]
            spans.append((offset, offset + len(payload) - 8))
    return spans


def check_exactly_once(label, ws, total_body):
    spans = body_spans(ws)
    cursor = HEADER_LEN
    for start, end in spans:
        if start != cursor:
            kind = "overlap" if start < cursor else "gap"
            check(label, False, f"{kind}: chunk at {start}, expected {cursor} (spans={spans})")
            return
        cursor = end
    check(label, cursor == HEADER_LEN + total_body,
          f"covered to {cursor}, expected {HEADER_LEN + total_body} (spans={spans})")


def new_session():
    session = GameSession("unittest")
    session.header[:] = b"H" * HEADER_LEN
    session.header_received = True
    session.body.extend(b"A" * INITIAL_BODY)
    session.delay_seconds = DELAY_SECONDS
    return session


async def test_join_then_append():
    """Observer joins; a live chunk is appended while catch-up is still pending.

    Without the send-lock handoff in add_observer(), the live chunk overtakes catch-up and
    the observer writes it past a hole.
    """
    print("\ntest_join_then_append")
    session = new_session()
    ws = FakeWS()

    send_lock = await session.add_observer(ws)
    check("add_observer returns a held lock", send_lock is not None and send_lock.locked())

    broadcast = asyncio.create_task(
        session.apply_body(ws, struct.pack("<Q", INITIAL_BODY) + b"B" * LIVE_CHUNK))
    for _ in range(10):
        await asyncio.sleep(0)   # let the broadcast reach the lock and block

    try:
        await session.send_catchup(ws, last_offset=0, held_lock=send_lock)
    finally:
        send_lock.release()
    await broadcast

    types = [t for t, _ in ws.frames]
    check("ROLE first", types[:1] == [MSG_ROLE], f"types={types}")
    check("HEADER second", types[1:2] == [MSG_HEADER], f"types={types}")
    check("ROLE carries delay_seconds",
          f'"delay_seconds":{DELAY_SECONDS}' in ws.frames[0][1].decode())
    check_exactly_once("body delivered exactly once", ws, INITIAL_BODY + LIVE_CHUNK)


async def test_append_then_join():
    """A chunk is appended before the observer registers.

    The catch-up snapshot already covers those bytes, so broadcasting them live as well
    would deliver them twice and rewind the client's parse cursor.
    """
    print("\ntest_append_then_join")
    session = new_session()
    ws = FakeWS()

    await session.apply_body(ws, struct.pack("<Q", INITIAL_BODY) + b"B" * LIVE_CHUNK)

    send_lock = await session.add_observer(ws)
    try:
        await session.send_catchup(ws, last_offset=0, held_lock=send_lock)
    finally:
        send_lock.release()

    check_exactly_once("body delivered exactly once", ws, INITIAL_BODY + LIVE_CHUNK)


async def test_games_payload():
    """/games must carry what the in-game observer browser renders."""
    print("\ntest_games_payload")
    session = new_session()
    session.lobby = {
        "lobbytype": 0,
        "region": "USWest",
        "rngseed": 1234,
        "mapname": "Tournament Desert",
        "mappath": "",
        "name": "Dennis's Game",
        "owner": 1,
        "members": [
            {"userid": 1, "displayname": "Dennis"},
            {"userid": 2, "displayname": "Opponent"},
            {"userid": -1, "displayname": ""},
        ],
    }

    saved = dict(server.games)
    server.games.clear()
    server.games["unittest"] = session
    try:
        rows = await server.list_games()
    finally:
        server.games.clear()
        server.games.update(saved)

    check("one row returned", len(rows) == 1, f"rows={rows}")
    row = rows[0]
    for key in ("lobbyid", "mapname", "name", "members", "viewers", "sources",
                "delay_seconds", "age_seconds"):
        check(f"row has {key}", key in row, f"row={row}")
    check("lobbyid populated", row.get("lobbyid") == "unittest", f"row={row}")
    check("map populated", row.get("mapname") == "Tournament Desert", f"row={row}")
    occupied = [m.get("displayname") for m in row.get("members", []) if m.get("userid", -1) != -1]
    check("players populated", occupied == ["Dennis", "Opponent"], f"row={row}")
    check("delay forwarded", row.get("delay_seconds") == DELAY_SECONDS, f"row={row}")
    check("age is a non-negative int",
          isinstance(row.get("age_seconds"), int) and row["age_seconds"] >= 0, f"row={row}")


async def test_ended_games_hidden():
    print("\ntest_ended_games_hidden")
    session = new_session()
    session.ended = True

    saved = dict(server.games)
    server.games.clear()
    server.games["unittest"] = session
    try:
        rows = await server.list_games()
    finally:
        server.games.clear()
        server.games.update(saved)

    check("ended games excluded", rows == [], f"rows={rows}")


async def main():
    await test_join_then_append()
    await test_append_then_join()
    await test_games_payload()
    await test_ended_games_hidden()

    print(f"\n{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
