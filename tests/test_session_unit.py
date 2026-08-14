#!/usr/bin/env python3
"""
Unit tests for GameSession delivery semantics. No server required.

Complements test_relay.py (which drives a running relay over the network) by exercising
the observer-join interleavings directly. Those are racy by nature and cannot be forced
reliably over a real socket, but they are exactly where a bug corrupts an observer's
replay file: observers write each BODY chunk at its absolute file offset, so a chunk
delivered out of order leaves a hole, and a chunk delivered twice rewinds the client's
parse cursor.

Run: python tests/test_session_unit.py
"""
import asyncio
import struct
import sys
import time
from pathlib import Path

# server.py lives at the repo root, one level up from tests/.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import server
from server import GameSession, MSG_ROLE, MSG_HEADER, MSG_BODY, MSG_END, MSG_TICK

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


class FakeClock:
    """Deterministic replacement for server.time.time in delay-hold tests.

    Installed with ClockPatch; every time.time() call inside the relay then reads
    clock.now, so arrival stamps, watermarks and flush deadlines are all exact.
    """

    def __init__(self, start: float = 1000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def tick(self, dt: float) -> None:
        self.now += dt


class ClockPatch:
    """Context manager swapping server.time.time for a FakeClock, restored on exit."""

    def __init__(self, clock: FakeClock):
        self.clock = clock
        self._real = server.time.time

    def __enter__(self):
        server.time.time = self.clock

    def __exit__(self, *exc):
        server.time.time = self._real


async def append_at(session, clock, ts, size=LIVE_CHUNK):
    """Append one body chunk with the clock at exactly `ts` (records an arrival stamp)."""
    clock.now = ts
    await session.apply_body(None, struct.pack("<Q", len(session.body)) + b"B" * size)


async def test_join_then_append():
    """Observer joins; a live chunk is appended while catch-up is still pending.

    Without the send-lock handoff in add_observer(), the live chunk overtakes catch-up and
    the observer writes it past a hole. Uses a priority observer (no delay hold) so the
    test isolates the lock machinery from the hold.
    """
    print("\ntest_join_then_append")
    session = new_session()
    ws = FakeWS()

    send_lock = await session.add_observer(ws, priority=True)
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

    send_lock = await session.add_observer(ws, priority=True)
    try:
        await session.send_catchup(ws, last_offset=0, held_lock=send_lock)
    finally:
        send_lock.release()

    check_exactly_once("body delivered exactly once", ws, INITIAL_BODY + LIVE_CHUNK)


# ── Byte-level delay hold (plans/relay/relay-server-side-delay-hold.md) ──────

async def test_delayed_watermark():
    """delayed_watermark returns the body length as of now - delay."""
    print("\ntest_delayed_watermark")
    session = GameSession("wm")
    session.body.extend(b"B" * 100)
    session.delay_seconds = DELAY_SECONDS
    for i in range(5):
        session._record_body_history(1010 + i * 10, 20 + i * 20)

    check("younger than delay -> 0", session.delayed_watermark(1042) == 0)
    check("cutoff at first entry", session.delayed_watermark(1052) == 20)
    check("cutoff between entries", session.delayed_watermark(1065) == 40)
    check("cutoff past last entry", session.delayed_watermark(1099) == 100)

    session.delay_seconds = 0
    check("delay 0 -> live edge", session.delayed_watermark(1042) == len(session.body))


async def test_held_observer_join_and_live_chunks():
    """A held observer starts at the delayed edge; younger bytes arrive at +delay."""
    print("\ntest_held_observer_join_and_live_chunks")
    clock = FakeClock()
    with ClockPatch(clock):
        session = GameSession("held")
        session.header[:] = b"H" * HEADER_LEN
        session.header_received = True
        session.delay_seconds = DELAY_SECONDS

        # Five chunks recorded at t=1000..1008 (body = 100), observer joins at t=1010.
        for i in range(5):
            await append_at(session, clock, 1000 + (i + 1) * 2)
        ws = FakeWS()
        send_lock = await session.add_observer(ws, priority=False)
        try:
            await session.send_catchup(ws, last_offset=0, held_lock=send_lock)
        finally:
            send_lock.release()

        check("observer is held", session._observer_held.get(ws, False))
        role = ws.frames[0][1].decode()
        check("held ROLE carries delay_seconds: 0", f'"delay_seconds":0' in role, role)
        check("no body in held catch-up", [t for t, _ in ws.frames if t == MSG_BODY] == [])

        # A chunk appended after join (t=1012, ready t=1054) must not be delivered yet.
        await append_at(session, clock, 1012)
        check("younger-than-delay chunk not delivered",
              [t for t, _ in ws.frames if t == MSG_BODY] == [])

        # The shared edge advances with the clock: watermark(1043) = 20, (1045) = 40 ...
        clock.tick(33)
        await session._flush_held_observers()
        clock.tick(2)
        await session._flush_held_observers()
        clock.tick(5)
        await session._flush_held_observers()
        check_exactly_once("delayed edge delivered 0-100", ws, 100)

        # The post-join chunk becomes available at its own arrival + delay (1054).
        clock.now = 1055
        await session._flush_held_observers()
        check_exactly_once("all 120 bytes delivered exactly once", ws, 120)


async def test_priority_observer_not_held():
    """A priority observer bypasses the hold: full catch-up, live chunks immediate."""
    print("\ntest_priority_observer_not_held")
    clock = FakeClock()
    with ClockPatch(clock):
        session = GameSession("prio")
        session.header[:] = b"H" * HEADER_LEN
        session.header_received = True
        session.delay_seconds = DELAY_SECONDS
        for i in range(5):
            await append_at(session, clock, 1000 + (i + 1) * 2)

        ws = FakeWS()
        send_lock = await session.add_observer(ws, priority=True)
        try:
            await session.send_catchup(ws, last_offset=0, held_lock=send_lock)
        finally:
            send_lock.release()

        check("priority observer not held", not session._observer_held.get(ws, False))
        check("priority ROLE keeps delay_seconds",
              f'"delay_seconds":{DELAY_SECONDS}' in ws.frames[0][1].decode())
        await append_at(session, clock, 1012)
        check_exactly_once("priority observer got everything immediately", ws, 120)


async def test_held_observer_end_flush():
    """Stream end flushes a held observer's remaining bytes immediately."""
    print("\ntest_held_observer_end_flush")
    clock = FakeClock()
    with ClockPatch(clock):
        session = GameSession("endflush")
        session.header[:] = b"H" * HEADER_LEN
        session.header_received = True
        session.delay_seconds = DELAY_SECONDS
        for i in range(5):
            await append_at(session, clock, 1000 + (i + 1) * 2)

        ws = FakeWS()
        send_lock = await session.add_observer(ws, priority=False)
        try:
            await session.send_catchup(ws, last_offset=0, held_lock=send_lock)
        finally:
            send_lock.release()

        await append_at(session, clock, 1012)   # not yet due (ready at 1054)
        await session._broadcast_envelope(MSG_END, b"", targets=[ws])

        check_exactly_once("END flushed the whole body", ws, 120)
        check("END frame sent after the flush",
              [t for t, _ in ws.frames].count(MSG_END) == 1)


async def test_delay_zero_not_held():
    """A delay-0 session holds nobody, even without a priority ticket."""
    print("\ntest_delay_zero_not_held")
    session = new_session()
    session.delay_seconds = 0
    ws = FakeWS()

    send_lock = await session.add_observer(ws, priority=False)
    check("delay 0 -> not held", not session._observer_held.get(ws, False))
    try:
        await session.send_catchup(ws, last_offset=0, held_lock=send_lock)
    finally:
        send_lock.release()

    await session.apply_body(ws, struct.pack("<Q", INITIAL_BODY) + b"B" * LIVE_CHUNK)
    check_exactly_once("delay 0 -> everything delivered", ws, INITIAL_BODY + LIVE_CHUNK)


# ── Frame heartbeat (MSG_TICK) ──────────────────────────────────────────────
#
# The tick tells an observer where the live game is without waiting for a record to show up
# in the body. Two properties matter and neither is visible from the byte stream, so they are
# tested here: it must not reach a delay-held observer (that observer is deliberately kept
# ignorant of the live edge), and it must never move backwards (several sources push the same
# stream, and an observer cannot un-simulate a frame it already ran).


def tick_frames(ws):
    """Frame numbers of the MSG_TICK frames delivered to ws, in order."""
    return [struct.unpack("<I", payload[:4])[0]
            for msg_type, payload in ws.frames if msg_type == MSG_TICK]


async def test_tick_forwarded_to_live_observer():
    """A live-edge observer receives ticks as they arrive."""
    print("\ntest_tick_forwarded_to_live_observer")
    session = new_session()
    session.delay_seconds = 0
    ws = FakeWS()

    send_lock = await session.add_observer(ws, priority=False)
    send_lock.release()

    await session.apply_tick(None, struct.pack("<I", 600))
    await session.apply_tick(None, struct.pack("<I", 610))

    check("ticks forwarded", tick_frames(ws) == [600, 610], tick_frames(ws))
    check("session remembers newest", session.last_tick_frame == 610)


async def test_tick_not_forwarded_to_held_observer():
    """A delay-held observer must not learn the live edge.

    The byte-level hold exists so a modified client cannot reach data younger than the
    delay. A tick carries no bytes, but it would hand over the very thing the hold is
    withholding: knowledge of where live is.
    """
    print("\ntest_tick_not_forwarded_to_held_observer")
    session = new_session()
    session.delay_seconds = DELAY_SECONDS
    ws = FakeWS()

    send_lock = await session.add_observer(ws, priority=False)
    send_lock.release()
    check("observer is held", session._observer_held.get(ws, False))

    await session.apply_tick(None, struct.pack("<I", 600))

    check("no tick delivered to held observer", tick_frames(ws) == [], tick_frames(ws))
    check("session still records it", session.last_tick_frame == 600)


async def test_tick_monotonic():
    """A stale tick from a lagging source never drags the advertised edge backwards."""
    print("\ntest_tick_monotonic")
    session = new_session()
    session.delay_seconds = 0
    ws = FakeWS()

    send_lock = await session.add_observer(ws, priority=False)
    send_lock.release()

    await session.apply_tick(None, struct.pack("<I", 900))
    await session.apply_tick(None, struct.pack("<I", 800))   # laggier source
    await session.apply_tick(None, struct.pack("<I", 900))   # duplicate

    check("only the forward tick forwarded", tick_frames(ws) == [900], tick_frames(ws))
    check("edge stays at the max", session.last_tick_frame == 900)


async def test_tick_in_catchup():
    """A joining observer gets the current edge immediately, after its catch-up body."""
    print("\ntest_tick_in_catchup")
    session = new_session()
    session.delay_seconds = 0
    await session.apply_tick(None, struct.pack("<I", 750))

    ws = FakeWS()
    send_lock = await session.add_observer(ws, priority=False)
    try:
        await session.send_catchup(ws, last_offset=0, held_lock=send_lock)
    finally:
        send_lock.release()

    check("catch-up carries the edge", tick_frames(ws) == [750], tick_frames(ws))

    # After the body it belongs to: a tick ahead of its records would assert an edge for
    # bytes the observer has not been given yet.
    types = [msg_type for msg_type, _ in ws.frames]
    check("tick follows the catch-up body",
          MSG_BODY in types and types.index(MSG_TICK) > max(
              i for i, t in enumerate(types) if t == MSG_BODY))


async def test_tick_ignored_from_demoted_source():
    """A demoted source has stopped pushing body data, so its edge claim is meaningless."""
    print("\ntest_tick_ignored_from_demoted_source")
    session = new_session()
    session.delay_seconds = 0
    ws = FakeWS()
    send_lock = await session.add_observer(ws, priority=False)
    send_lock.release()

    src = register_source(session, "A")
    session._source_health[src]["demoted"] = True

    await session.apply_tick(src, struct.pack("<I", 600))

    check("demoted source's tick dropped", tick_frames(ws) == [], tick_frames(ws))
    check("edge unchanged", session.last_tick_frame == 0)


# ── All-push demotion / re-promotion (plans/relay/streamer-allpush-demotion.md) ──

def register_source(session, name):
    """Register a fake source with its own health dict, as stream_endpoint does."""
    ws = FakeWS()
    session.sources.add(ws)
    session._source_health[ws] = {
        "demoted": False,
        "lag_bytes": 0,
        "gap_strikes": 0,
        "mismatch_strikes": 0,
        "frames_seen": 0,
        "last_frame_at": time.time(),
        "body_len_seen": len(session.body),
    }
    return ws


async def test_demote_on_lag_threshold():
    """A source whose cumulative lag crosses the threshold is demoted to backup."""
    print("\ntest_demote_on_lag_threshold")
    session = new_session()
    a = register_source(session, "A")
    b = register_source(session, "B")

    # Small chunk sizes in tests: use a tiny threshold so cumulative lag crosses it fast.
    old_threshold = server.SOURCE_LAG_BYTES
    server.SOURCE_LAG_BYTES = LIVE_CHUNK * 5

    # A streams live; B always arrives exactly one chunk behind (lag == LIVE_CHUNK per frame).
    for i in range(10):
        chunk = struct.pack("<Q", INITIAL_BODY + i * LIVE_CHUNK) + b"B" * LIVE_CHUNK
        await session.apply_body(a, chunk)
        session._touch_source(a)
        # B repeats the previous chunk -> overlap, lag = LIVE_CHUNK
        lagged = struct.pack("<Q", INITIAL_BODY + i * LIVE_CHUNK) + b"B" * LIVE_CHUNK
        await session.apply_body(b, lagged)
        session._touch_source(b)

    check("B accumulated lag", session._source_health[b]["lag_bytes"] == 10 * LIVE_CHUNK)
    check("A accumulated no lag", session._source_health[a]["lag_bytes"] == 0)

    demoted = await session._maybe_demote_source(b)
    check("B demoted on lag", demoted)
    check("B flagged demoted", session._source_health[b]["demoted"])

    # Demoted source's frames are ignored.
    before = len(session.body)
    await session.apply_body(b, struct.pack("<Q", len(session.body)) + b"X" * LIVE_CHUNK)
    check("demoted source frames ignored", len(session.body) == before)

    # A still streams.
    await session.apply_body(a, struct.pack("<Q", len(session.body)) + b"C" * LIVE_CHUNK)
    check("active source still appends", len(session.body) == before + LIVE_CHUNK)

    server.SOURCE_LAG_BYTES = old_threshold


async def test_demote_on_gap_strikes():
    """A source that repeatedly skips ahead (gap) accumulates strikes and is demoted."""
    print("\ntest_demote_on_gap_strikes")
    session = new_session()
    a = register_source(session, "A")
    b = register_source(session, "B")

    # B sends frames with offsets that jump past body_len.
    for i in range(server.SOURCE_GAP_STRIKES):
        await session.apply_body(b, struct.pack("<Q", INITIAL_BODY + 1000 + i * 100) + b"G" * 10)
        session._touch_source(b)

    check("B accumulated gap strikes", session._source_health[b]["gap_strikes"] == server.SOURCE_GAP_STRIKES)

    demoted = await session._maybe_demote_source(b)
    check("B demoted on gap strikes", demoted)


async def test_never_demote_last_active_source():
    """The last active pusher is never demoted, even when its health is terrible."""
    print("\ntest_never_demote_last_active_source")
    session = new_session()
    a = register_source(session, "A")

    session._source_health[a]["gap_strikes"] = 999
    session._source_health[a]["lag_bytes"] = 10_000_000

    demoted = await session._maybe_demote_source(a)
    check("last active source not demoted", not demoted)
    check("still streaming", not session._source_health[a]["demoted"])

    # With two sources, A can be demoted even if B is silent-but-present.
    b = register_source(session, "B")
    demoted = await session._maybe_demote_source(a)
    check("demotable once a second source exists", demoted)


async def test_promote_backup_when_active_leaves():
    """When the last active pusher leaves, the least-bad backup is re-promoted.

    The takeover ROLE must carry the current body offset so the backup can backfill
    from its local recording.
    """
    print("\ntest_promote_backup_when_active_leaves")
    session = new_session()
    a = register_source(session, "A")
    b = register_source(session, "B")

    old_threshold = server.SOURCE_LAG_BYTES
    server.SOURCE_LAG_BYTES = LIVE_CHUNK * 5

    session._source_health[b]["lag_bytes"] = 50 * LIVE_CHUNK
    await session.apply_body(a, struct.pack("<Q", INITIAL_BODY) + b"B" * LIVE_CHUNK)
    session._touch_source(a)

    demoted = await session._maybe_demote_source(b)
    check("B was demoted", demoted)

    # A (the only active pusher) leaves.
    session.sources.discard(a)
    promoted = await session._maybe_promote_backup()
    check("backup promoted", promoted)
    check("B back to active", not session._source_health[b]["demoted"])

    role_frame = None
    for msg_type, payload in b.frames:
        if msg_type == MSG_ROLE:
            role_frame = payload.decode()
    check("takeover ROLE sent", role_frame is not None and "takeover" in (role_frame or ""))
    check("takeover carries body_offset",
          f'"body_offset":{INITIAL_BODY + LIVE_CHUNK}' in (role_frame or ""))

    server.SOURCE_LAG_BYTES = old_threshold


async def test_promote_picks_least_bad_backup():
    """Re-promotion chooses the backup with the best health, not the most recent."""
    print("\ntest_promote_picks_least_bad_backup")
    session = new_session()
    a = register_source(session, "A")
    good = register_source(session, "good")
    bad = register_source(session, "bad")

    old_threshold = server.SOURCE_LAG_BYTES
    server.SOURCE_LAG_BYTES = LIVE_CHUNK * 5

    # Both backups misbehave, but 'bad' much worse.
    session._source_health[good]["lag_bytes"] = 5 * LIVE_CHUNK
    session._source_health[bad]["lag_bytes"] = 200 * LIVE_CHUNK
    await session._maybe_demote_source(good)
    await session._maybe_demote_source(bad)

    session.sources.discard(a)
    await session._maybe_promote_backup()

    check("least-bad backup promoted", not session._source_health[good]["demoted"])
    check("worse backup stays demoted", session._source_health[bad]["demoted"])

    server.SOURCE_LAG_BYTES = old_threshold


async def test_no_promotion_when_active_remains():
    """A source leaving while another active pusher remains does not promote anyone."""
    print("\ntest_no_promotion_when_active_remains")
    session = new_session()
    a = register_source(session, "A")
    b = register_source(session, "B")

    session.sources.discard(a)
    promoted = await session._maybe_promote_backup()
    check("no promotion while active source remains", not promoted)


async def main():
    await test_join_then_append()
    await test_append_then_join()
    await test_delayed_watermark()
    await test_held_observer_join_and_live_chunks()
    await test_priority_observer_not_held()
    await test_held_observer_end_flush()
    await test_delay_zero_not_held()
    await test_tick_forwarded_to_live_observer()
    await test_tick_not_forwarded_to_held_observer()
    await test_tick_monotonic()
    await test_tick_in_catchup()
    await test_tick_ignored_from_demoted_source()
    await test_demote_on_lag_threshold()
    await test_demote_on_gap_strikes()
    await test_never_demote_last_active_source()
    await test_promote_backup_when_active_leaves()
    await test_promote_picks_least_bad_backup()
    await test_no_promotion_when_active_remains()

    print(f"\n{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
