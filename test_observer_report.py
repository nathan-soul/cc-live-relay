#!/usr/bin/env python3
"""
Unit tests for the relay's batched livestream-state reporting to GO services.

No server required. The shared batch timer and dirty sets are exercised directly; the outbound
POST is stubbed so nothing touches the network.

Run: python test_observer_report.py
"""
import asyncio
import sys

import server
from server import GameSession

PASS = 0
FAIL = 0


class FakeWS:
    async def send_bytes(self, data: bytes):
        await asyncio.sleep(0)


def check(label: str, condition: bool, detail: str = ""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS  {label}")
    else:
        FAIL += 1
        print(f"  FAIL  {label}  {detail}")


def patch(attr: str, value):
    setattr(server, attr, value)


async def test_batch_coalesces_multiple_lobbies():
    """Changes across different lobbies within the window flush as one batch."""
    print("\ntest_batch_coalesces_multiple_lobbies")
    patch("GO_OBSERVERS_URL", "http://go/observers")
    patch("OBSERVER_CHANGE_TIMEOUT", 0.05)
    sent = []
    original = server.notify_lobby_progress

    async def stub(entries):
        sent.append(entries)

    server.notify_lobby_progress = stub
    try:
        s1 = GameSession("batch_001")
        s2 = GameSession("batch_002")
        server.games["batch_001"] = s1
        server.games["batch_002"] = s2
        ws1, ws2, ws3 = FakeWS(), FakeWS(), FakeWS()

        await s1.add_observer(ws1)
        await s1.add_observer(ws2)
        await s2.add_observer(ws3)
        await s1.remove_observer(ws1)
        # All within the window: one shared timer, one batch.
        check("exactly one pending batch task", server._observer_batch_task is not None)

        await asyncio.sleep(0.15)  # > OBSERVER_CHANGE_TIMEOUT
        check("one batch with both lobbies", len(sent) == 1, f"sent={sent}")
        check("entries carry is_live=True", all(e["is_live"] is True for e in sent[0]), f"sent={sent}")
        expected = [
            {"lobby_id": "batch_001", "observer_count": 1, "is_live": True},
            {"lobby_id": "batch_002", "observer_count": 1, "is_live": True},
        ]
        check("batch contains both counts", sorted(sent[0], key=lambda e: e["lobby_id"]) == sorted(expected, key=lambda e: e["lobby_id"]),
              f"got={sent}")
        check("dirty sets cleared", server._observer_dirty == set() and server._ended_dirty == set())
        check("pending task cleared", server._observer_batch_task is None)
    finally:
        server.notify_lobby_progress = original
        server.games.pop("batch_001", None)
        server.games.pop("batch_002", None)
        server._observer_dirty.clear()
        server._ended_dirty.clear()
        patch("GO_OBSERVERS_URL", "")
        patch("OBSERVER_CHANGE_TIMEOUT", 15)


async def test_timer_not_reset_by_mid_window_change():
    """A change arriving mid-window must not extend the batch's fire time."""
    print("\ntest_timer_not_reset_by_mid_window_change")
    patch("GO_OBSERVERS_URL", "http://go/observers")
    patch("OBSERVER_CHANGE_TIMEOUT", 0.10)
    sent = []
    original = server.notify_lobby_progress

    async def stub(entries):
        sent.append(entries)

    server.notify_lobby_progress = stub
    try:
        session = GameSession("batch_mid")
        server.games["batch_mid"] = session
        ws1, ws2 = FakeWS(), FakeWS()

        await session.add_observer(ws1)          # arms the timer at t=0
        await asyncio.sleep(0.06)                # halfway through the window
        await session.add_observer(ws2)          # arrives mid-window: must NOT reset the timer
        # If the timer had been reset, it would fire at ~0.16s; as-is it fires at ~0.10s.
        await asyncio.sleep(0.06)                # total 0.12s > original 0.10s window

        expected = [{"lobby_id": "batch_mid", "observer_count": 2, "is_live": True}]
        check("batch fired on original schedule", sent == [expected], f"sent={sent}")
        check("single batch, both changes coalesced", len(sent) == 1, f"sent={sent}")
    finally:
        server.notify_lobby_progress = original
        server.games.pop("batch_mid", None)
        server._observer_dirty.clear()
        server._ended_dirty.clear()
        patch("GO_OBSERVERS_URL", "")
        patch("OBSERVER_CHANGE_TIMEOUT", 15)


async def test_unchanged_count_skipped():
    """A lobby whose count is unchanged from the last report is left out of the batch."""
    print("\ntest_unchanged_count_skipped")
    patch("GO_OBSERVERS_URL", "http://go/observers")
    patch("OBSERVER_CHANGE_TIMEOUT", 0.05)
    sent = []
    original = server.notify_lobby_progress

    async def stub(entries):
        sent.append(entries)

    server.notify_lobby_progress = stub
    try:
        session = GameSession("batch_skip")
        server.games["batch_skip"] = session
        ws1, ws2 = FakeWS(), FakeWS()

        await session.add_observer(ws1)
        await session.add_observer(ws2)
        await session.remove_observer(ws1)
        await asyncio.sleep(0.15)
        check("first batch reports count 1", sent == [[{"lobby_id": "batch_skip", "observer_count": 1, "is_live": True}]], f"sent={sent}")

        # Add+remove returns the count to 1 (== last reported) -> nothing new to post.
        await session.add_observer(ws1)
        await session.remove_observer(ws1)
        await asyncio.sleep(0.15)
        check("no redundant batch for unchanged count", len(sent) == 1, f"sent={sent}")
    finally:
        server.notify_lobby_progress = original
        server.games.pop("batch_skip", None)
        server._observer_dirty.clear()
        server._ended_dirty.clear()
        patch("GO_OBSERVERS_URL", "")
        patch("OBSERVER_CHANGE_TIMEOUT", 15)


async def test_ended_stream_sends_is_live_false():
    """mark_stream_ended emits is_live=False with count 0, and it wins over any count change."""
    print("\ntest_ended_stream_sends_is_live_false")
    patch("GO_OBSERVERS_URL", "http://go/observers")
    patch("OBSERVER_CHANGE_TIMEOUT", 0.05)
    sent = []
    original = server.notify_lobby_progress

    async def stub(entries):
        sent.append(entries)

    server.notify_lobby_progress = stub
    try:
        session = GameSession("batch_ended")
        server.games["batch_ended"] = session
        ws1 = FakeWS()
        await session.add_observer(ws1)          # count change
        server.mark_stream_ended("batch_ended")  # then the stream closes before the flush

        await asyncio.sleep(0.15)
        expected = [{"lobby_id": "batch_ended", "observer_count": 0, "is_live": False}]
        check("single ended entry sent", sent == [expected], f"sent={sent}")
        check("dirty sets cleared", server._observer_dirty == set() and server._ended_dirty == set())
    finally:
        server.notify_lobby_progress = original
        server.games.pop("batch_ended", None)
        server._observer_dirty.clear()
        server._ended_dirty.clear()
        patch("GO_OBSERVERS_URL", "")
        patch("OBSERVER_CHANGE_TIMEOUT", 15)


async def test_periodic_flush():
    """The periodic loop drains the same dirty sets into a batch."""
    print("\ntest_periodic_flush")
    patch("GO_OBSERVERS_URL", "http://go/observers")
    patch("OBSERVER_UPDATE_INTERVAL", 0.05)
    sent = []
    original = server.notify_lobby_progress

    async def stub(entries):
        sent.append(entries)

    server.notify_lobby_progress = stub
    try:
        session = GameSession("batch_periodic")
        server.games["batch_periodic"] = session
        ws1 = FakeWS()
        await session.add_observer(ws1)

        loop = asyncio.create_task(server._observer_report_loop())
        await asyncio.sleep(0.13)  # two full intervals
        loop.cancel()
        try:
            await loop
        except asyncio.CancelledError:
            pass

        expected = [{"lobby_id": "batch_periodic", "observer_count": 1, "is_live": True}]
        check("periodic baseline flushed the batch", sent == [expected], f"sent={sent}")
        # Second tick: count unchanged -> no redundant post.
        check("no redundant post for unchanged count", len(sent) == 1, f"sent={sent}")
    finally:
        server.notify_lobby_progress = original
        server.games.pop("batch_periodic", None)
        server._observer_dirty.clear()
        server._ended_dirty.clear()
        patch("GO_OBSERVERS_URL", "")
        patch("OBSERVER_UPDATE_INTERVAL", 60)


async def main():
    await test_batch_coalesces_multiple_lobbies()
    await test_timer_not_reset_by_mid_window_change()
    await test_unchanged_count_skipped()
    await test_ended_stream_sends_is_live_false()
    await test_periodic_flush()

    print(f"\n{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
