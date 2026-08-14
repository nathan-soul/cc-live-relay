"""
cc-live-relay — Live game relay server for Generals Zero Hour

Architecture: GO Services → Relay → Observer/Streamer
GO services validates user JWTs and calls this relay over HTTP with a shared
INTERNAL_API_KEY to mint single-use stream tokens (streamers) and watch tickets
(observers). This relay never sees a user JWT; it only trusts GO. See
plans/relay/archive/relay-go-orchestrated-livestreams.md for the original design (parts of it
are superseded; docs/running-the-stack.md is current).

WebSocket-based relay with binary envelope protocol (msg types 0-8).
Aligned with the C++ LiveStreamer/LiveObserver client (libcurl websockets).
"""

import asyncio
import hmac
import json
import os
import secrets
import struct
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import aiohttp
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect

# ── Binary message types (aligned with C++ client) ────────────────────────
MSG_REGISTER = 0
MSG_HEADER   = 1
MSG_PATCH    = 2
MSG_BODY     = 3
MSG_END      = 4
MSG_ROLE     = 5
MSG_ERROR    = 6
MSG_CHAT     = 7
MSG_SPECTATOR_CHAT = 8
# Frame heartbeat (plans/relay/live-observer-frame-heartbeat.md): the source's current logic
# frame, [frame u32 LE]. The replay body only contains records for frames that have input, so
# in quiet play it is silent apart from one CRC record every ~1.7 s; an observer deriving the
# live edge from those records learns where the game is in 1.7 s jumps and starves between
# them. The tick states the frame directly. It is opaque to the relay, which forwards it and
# remembers the latest value for observers joining later.
MSG_TICK     = 9

CHUNK_SIZE = 256 * 1024  # 256 KB per chunk for observer catch-up

# ── Chat (plans/relay/live-observer-chat.md + live-observer-spectator-chat.md) ──
# MSG_CHAT = player chat, frame-stamped by the streamer, frame-gated on the observer;
# the relay stores a bounded history so late-joining watchers get the recent slice in
# their catch-up. MSG_SPECTATOR_CHAT = live spectator meta-chat (Twitch-style): no
# history by design ("you missed it"), rate-limited per connection.
CHAT_HISTORY_MAX = 200          # bounded chat_history per session (player chat)
CHAT_CATCHUP_COUNT = 10         # last-N chat frames sent to a joining observer
SPECTATOR_RATE_MAX = 5          # spectator messages allowed per window...
SPECTATOR_RATE_WINDOW = 10      # ...per this many seconds, per connection

# ── Configuration via environment variables ────────────────────────────────
PORT = int(os.getenv("PORT", "8765"))
MAX_OBSERVERS_PER_GAME = int(os.getenv("MAX_OBSERVERS_PER_GAME", "1000"))
INACTIVE_GAME_TTL = 60
# How long a session may exist without the host ever describing it before it is dropped. Only
# the host's REGISTER carries the lobby block, so until it arrives the game cannot be listed or
# meaningfully watched — see _cleanup_loop.
UNDESCRIBED_GAME_TTL = int(os.getenv("UNDESCRIBED_GAME_TTL", "120"))

# Broadcast delay: how far behind live an observer is held. The host owns this value (it is
# their spoiler window) and reports it to GO, which forwards it on POST /internal/livestreams;
# the relay then sends it to every observer before any replay data. Used when neither GO nor
# the host's REGISTER carries a delay (older build, or nothing sent).
DEFAULT_DELAY_SECONDS = int(os.getenv("DEFAULT_DELAY_SECONDS", "15"))
MAX_DELAY_SECONDS = 600

# Byte-level delay hold (plans/relay/relay-server-side-delay-hold.md): a normal viewer of a
# delayed stream only ever receives body bytes older than the host's delay, so a modified
# client cannot fast-forward past the delayed data edge — the bytes do not exist yet. The
# hold is a single shared delayed edge per session (arrival history + watermark), not
# per-observer buffering: every held observer just tracks how far it has received. A global
# ticker delivers chunks whose delay elapsed (flush-on-append covers the common case).
DELAY_FLUSH_INTERVAL = float(os.getenv("DELAY_FLUSH_INTERVAL", "1.0"))
# Hard cap on arrival-history entries (one per appended BODY frame), in case a pathological
# source appends far faster than real-time recording would. The 2x-delay time trim below is
# the real bound; this is a ceiling for extreme cases.
BODY_HISTORY_MAX = 50_000

# Max concurrent per-chunk observer sends in _broadcast_envelope. At scale (many games x many
# observers) an unbounded per-observer task per BODY chunk creates tens of thousands of tasks a
# second, which can OOM the container. A bounded cap keeps the fan-out concurrent but limits the
# task churn. Lower = gentler on memory, higher = lower per-observer tail latency.
BROADCAST_CONCURRENCY = int(os.getenv("BROADCAST_CONCURRENCY", "256"))

# ── Per-source health / demotion (all-push model, plans/relay/streamer-allpush-demotion.md) ──
# Every can_stream player pushes; the relay watches each source for problems and tells a bad
# source to become a backup (role=backup) when it crosses any threshold below, as long as at
# least one other active pusher remains (the last pusher is never demoted, so the stream can
# never die from demotion alone). A demoted source stays connected and keeps recording; if the
# last active pusher later leaves, the relay re-promotes the least-bad backup via a takeover
# ROLE with the current body offset (see _promote_if_no_active).
# SOURCE_LAG_BYTES: average lag per BODY frame (bytes behind the live edge at delivery).
# SOURCE_GAP_STRIKES: BODY frames whose offset jumped past body_len.
# SOURCE_SILENCE_SECONDS: how long a source may stay silent while the body advances.
SOURCE_LAG_BYTES = int(os.getenv("SOURCE_LAG_BYTES", str(64 * 1024)))
SOURCE_GAP_STRIKES = int(os.getenv("SOURCE_GAP_STRIKES", "3"))
SOURCE_SILENCE_SECONDS = int(os.getenv("SOURCE_SILENCE_SECONDS", "10"))

# Verbose per-game / per-connection logging. Enable with DEBUG=1 (or "true"/"yes"/"on").
DEBUG = os.getenv("DEBUG", "").strip().lower() in ("1", "true", "yes", "on")

# ── GO-orchestration config (see docs/running-the-stack.md) ─────────────────
# Shared secret GO services sends as X-Relay-Key on every /internal/* call. Without it the
# internal endpoints refuse to serve (503), so a misconfigured deploy fails loudly instead of
# accepting unauthenticated mint requests. This is GO's credential to this relay — the relay
# does not send this key out.
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY", "")
# Credential lifetime. GO deliberately does not send an expiry: the client flow is atomic
# (GO validates JWT -> mints -> sends -> client connects immediately), so a short fixed window
# is plenty and keeps the stolen-ticket replay window small. A long match never needs a
# mid-stream refresh because the ticket is consumed on connect.
WATCH_TICKET_TTL_SECONDS = int(os.getenv("WATCH_TICKET_TTL_SECONDS", "30"))
# Public scheme/host used when building the URLs GO hands to clients. Falls back to the
# request's own Host header, which is trustworthy here only because the relay binds directly
# (no untrusted reverse proxy in front of it). Set PUBLIC_HOST explicitly if that changes.
PUBLIC_HOST = os.getenv("PUBLIC_HOST", "")
PUBLIC_WS_SCHEME = os.getenv("PUBLIC_WS_SCHEME", "wss")
# Optional path prefix on the public URLs the relay hands to clients, for when the relay is
# served behind a reverse proxy under a sub-path rather than a dedicated hostname — e.g.
# `wss://batty.youbantoo.club/relay/stream/...` when Traefik routes `/relay/*` to this relay.
# The relay's own WS routes (/stream, /watch) are NOT prefixed; the reverse proxy strips the
# prefix before forwarding, and this value only makes the minted connect URLs match the
# public path. Empty by default (no prefix).
PUBLIC_PATH_PREFIX = os.getenv("PUBLIC_PATH_PREFIX", "")

# The relay's own secret for outbound calls to GO (the batched livestream-progress
# notification). Distinct from INTERNAL_API_KEY by design: each side has its own credential,
# so compromising one side does not reveal the key the other side uses. GO stores the same
# value as Relay.ingress_api_key.
GO_API_KEY = os.getenv("GO_API_KEY", "")
# Where GO services receives the relay's batched livestream-progress updates (POST /observers):
# an array of {lobby_id, observer_count, is_live} entries. The relay is the only party that
# knows who is actually connected as an observer and when a stream truly closed, so it reports
# a rough, coalesced snapshot here rather than per-event liveness. Optional: empty means the
# relay never reports to GO. Authenticated with GO_API_KEY as X-Relay-Key.
GO_OBSERVERS_URL = os.getenv("GO_OBSERVERS_URL", "")
# How often (seconds) the relay pushes each active game's observer count to GO, as a baseline
# even when nothing changed.
OBSERVER_UPDATE_INTERVAL = int(os.getenv("OBSERVER_UPDATE_INTERVAL", "60"))
# Window (seconds) after the first observer join/leave (or a stream ending) before the relay
# posts the batch to GO. Changes that arrive before the timer fires are coalesced into that
# same post; the timer is never reset. The count is a rough estimate -- not live, not per-event.
OBSERVER_CHANGE_TIMEOUT = int(os.getenv("OBSERVER_CHANGE_TIMEOUT", "15"))

# Shared outbound HTTP session for GO notifications. Created once at first use and reused for
# every livestream-progress POST, so the TLS/TCP setup cost is paid once per process instead of
# once per stream. Guarded by the asyncio event loop, which runs single-threaded, so lazy
# creation here is race-free.
_go_notify_session: Optional[aiohttp.ClientSession] = None


def log_debug(*args) -> None:
    """Print a debug log line only when DEBUG is enabled."""
    if DEBUG:
        print(*args)


def log_warn(*args) -> None:
    """Print a warning or error. Always shown, regardless of DEBUG.

    Per-frame traffic (HEADER/PATCH/BODY/CATCHUP) is noise and belongs behind DEBUG, but
    these are rare and mean something is wrong -- a dropped body chunk, a desync, an
    unhandled exception in a handler. An operator running without DEBUG still needs them;
    the BODY gap message even asks the reader to investigate the source, which it cannot
    do if nobody ever sees it.
    """
    print(*args)


# ── App ────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Run the background loops for as long as the app is serving.

    The loops are held in a list rather than fired and forgotten: asyncio keeps only a weak
    reference to a running task, so one nobody holds can be garbage-collected mid-flight.
    Cancelling them on the way out (along with any pending observer batch) lets the process
    exit promptly instead of sitting on a sleep.

    _cleanup_loop and _observer_report_loop are defined further down the module; the names
    resolve when this body runs at startup, not when the app is constructed.
    """
    background = [
        asyncio.create_task(_cleanup_loop()),
        asyncio.create_task(_observer_report_loop()),
        asyncio.create_task(_delay_flush_loop()),
    ]
    try:
        yield
    finally:
        global _go_notify_session, _observer_batch_task

        if _observer_batch_task is not None:
            background.append(_observer_batch_task)
            _observer_batch_task = None

        for task in background:
            task.cancel()
        await asyncio.gather(*background, return_exceptions=True)

        # The relay is going down (deploy/restart/stop). Every healthy session is being
        # torn out from under its clients, so tell them: sources get an ERROR frame and
        # their sockets closed (a graceful game end never looks like this), observers get
        # the same ERROR so they can show "stream lost" in-game instead of waiting out
        # their own watchdog on a dead socket. Never raises — shutdown must not fail.
        for session in list(games.values()):
            if session.ended:
                continue
            try:
                reason_json = json.dumps({"reason": "relay-shutdown",
                                          "msg": "relay is going down"},
                                         separators=(',', ':'))
                await session._broadcast_envelope(MSG_ERROR, reason_json.encode())
            except Exception:
                pass
            for ws in list(session.sources):
                try:
                    await ws.close()
                except Exception:
                    pass
        if games:
            log_warn(f"[LIVESTREAMER] [SHUTDOWN] Relay shutting down with {len(games)} live "
                     f"session(s); notified their observers and sources")

        # Close the shared outbound session so the process exits cleanly.
        if _go_notify_session is not None:
            await _go_notify_session.close()
            _go_notify_session = None


app = FastAPI(title="cc-live-relay", version="0.7.0", lifespan=lifespan)


# ── Binary envelope helpers ────────────────────────────────────────────────

def pack_frame(msg_type: int, payload: bytes = b"") -> bytes:
    """1-byte type + 4-byte length (uint32 LE) + payload."""
    return bytes([msg_type]) + struct.pack("<I", len(payload)) + payload


def unpack_frame(data: bytes) -> tuple:
    """Unpack binary frame. Returns (msg_type, payload) or (None, b"") on error."""
    if len(data) < 5:
        return (None, b"")
    msg_type = data[0]
    payload_len = struct.unpack("<I", data[1:5])[0]
    if len(data) < 5 + payload_len:
        return (None, b"")
    return (msg_type, data[5:5 + payload_len])


async def reject(websocket: WebSocket, message: str) -> None:
    """Send an MSG_ERROR frame and close the WebSocket — the common admission-failure path."""
    try:
        await websocket.send_bytes(pack_frame(MSG_ERROR, message.encode()))
    except Exception:
        pass
    try:
        await websocket.close()
    except Exception:
        pass


# ── GO-shaped lobby metadata ───────────────────────────────────────────────
#
# The client sends the descriptive half of its GeneralsOnline lobby verbatim under "lobby" in
# REGISTER, using GO's own key spelling, and the relay republishes it. A client therefore
# parses the same structure whether a game list came from here or, one day, from GO itself.
#
# The allow-lists are the point of this, not a formality: a GO lobby also carries a password,
# per-member ports and an anticheat id, none of which are a third-party viewer's business. Only
# these keys survive into a session, so the relay can never become an accidental republisher of
# something a client should not have sent in the first place.
LOBBY_KEYS = ("lobbytype", "region", "rngseed", "mapname", "mappath", "name", "owner")
LOBBY_MEMBER_KEYS = ("userid", "displayname")


def sanitize_lobby(raw) -> dict:
    """Reduce a client-sent lobby block to the allow-listed keys. Never raises."""
    if not isinstance(raw, dict):
        return {}
    lobby = {k: raw[k] for k in LOBBY_KEYS if k in raw}
    members = []
    raw_members = raw.get("members")
    if isinstance(raw_members, list):
        for member in raw_members:
            if isinstance(member, dict):
                members.append({k: member[k] for k in LOBBY_MEMBER_KEYS if k in member})
    lobby["members"] = members
    return lobby


def parse_delay_seconds(raw) -> Optional[int]:
    """Clamp a supplied broadcast delay into [0, MAX_DELAY_SECONDS]. None if unusable.

    Shared by both sources of the value — GO on /internal/livestreams and the host's REGISTER
    frame — so a delay is bounded identically no matter which path it arrived on.
    """
    try:
        return max(0, min(int(raw), MAX_DELAY_SECONDS))
    except (TypeError, ValueError):
        return None


def lobby_player_names(lobby: dict) -> list:
    """Display names of the occupied slots only.

    members[] mirrors GO exactly, which means it includes the empty slots (userid -1, blank
    display name) that pad a lobby out to its maximum size. Those are meaningless in a
    "who is playing" list, so they are dropped here rather than at the transport.
    """
    names = []
    for member in lobby.get("members") or []:
        if not isinstance(member, dict):
            continue
        if member.get("userid", -1) == -1:
            continue
        name = member.get("displayname", "")
        if isinstance(name, str) and name and name not in names:
            names.append(name)
    return names


# ── GameSession ────────────────────────────────────────────────────────────

class GameSession:
    """One active game: multiple sources, multiple observers."""

    def __init__(self, lobby_id: str):
        # GO's LobbyID as decimal text. The relay's session key, the id an observer watches
        # by, and the same value GO itself publishes — one id, and one name for it.
        self.lobby_id: str = lobby_id
        # GO-shaped lobby block, first registrant wins (see sanitize_lobby). The single
        # source of descriptive truth for this session — deliberately not unpacked into
        # separate map/player fields, so there is nothing to keep in step with it.
        self.lobby: dict = {}
        self.created_at: float = time.time()
        self.last_active: float = time.time()
        self.delay_seconds: int = DEFAULT_DELAY_SECONDS
        # Whether delay_seconds came from GO (POST /internal/livestreams). GO is authoritative:
        # it takes the value from the host when the host registers the stream, and it is settled
        # before any source connects. The host's REGISTER frame is only a fallback for a
        # deployment whose GO does not send one, so it must not override this.
        self.delay_from_go: bool = False

        self.header: bytearray = bytearray()
        self.header_received: bool = False
        self.body: bytearray = bytearray()
        self.ended: bool = False
        self.end_received: bool = False

        self.sources: set[WebSocket] = set()
        self.observer_ws_set: set[WebSocket] = set()
        # GO account that registered the livestream (set via /internal/livestreams). Identity
        # info is GO's business; the relay stores it only so admission checks could bind to it.
        self.owner_user_id: Optional[int] = None

        # Per-source health for the all-push demotion model (plans/relay/streamer-allpush-demotion.md).
        # Keyed by the source's WebSocket; populated at registration, updated as frames arrive.
        # A source whose health crosses a threshold is told role=backup and its frames are
        # ignored; it may be re-promoted later if the last active pusher leaves.
        self._source_health: dict[WebSocket, dict] = {}

        # Per-source REGISTER flag: whether this source is an in-game observer (Side 1,
        # not an active player). Spectator chat reaches observer-mode sources only;
        # streaming players never see it (plans/relay/live-observer-spectator-chat.md).
        self._source_observer: dict[WebSocket, bool] = {}

        # Player chat history (MSG_CHAT, frame-stamped): bounded, opaque payloads.
        # The last CHAT_CATCHUP_COUNT entries are sent to a joining observer.
        self.chat_history: deque = deque(maxlen=CHAT_HISTORY_MAX)

        # Spectator chat rate limiting (MSG_SPECTATOR_CHAT): per-connection timestamps.
        self._spectator_rate: dict[WebSocket, list] = {}

        self._lock = asyncio.Lock()
        self._observer_send_locks: dict[WebSocket, asyncio.Lock] = {}
        # Where an observer's delivered body ends. For a priority observer (or a delay-0
        # session) it is the live edge at registration — live broadcasts carry on from it.
        # For a held observer it is the delayed edge at registration and doubles as its
        # delivery pointer: the shared flush advances it to the current watermark, so each
        # byte is delivered exactly once, and the catch-up cap keeps live chunks from ever
        # duplicating catch-up bytes (plans/relay/relay-server-side-delay-hold.md).
        self._observer_catchup_limit: dict[WebSocket, int] = {}
        # Whether this observer is held behind the broadcast delay: non-priority watchers
        # of a delayed session (priority = GO-stamped admin / user_priority=Viewer on the
        # watch ticket). Held observers never receive BODY chunks directly — the shared
        # delayed edge (see _flush_held_observers) delivers them at arrival + delay.
        self._observer_held: dict[WebSocket, bool] = {}
        # (arrival_ts, body_len) per appended BODY frame, trimmed to a 2x-delay window.
        # One append per frame — no replay parsing. Feeds delayed_watermark().
        self._body_history: deque = deque()
        # Last count actually posted to GO, so unchanged sessions skip redundant posts.
        self._last_reported_observers: Optional[int] = None
        # Newest frame heartbeat seen from any source (MSG_TICK). Kept so an observer that
        # joins between ticks gets the live edge immediately instead of waiting for the next
        # one. Monotonic: several sources push the same stream and a laggier one must never
        # drag the edge backwards.
        self.last_tick_frame: int = 0

    # ── Data ingestion (called from source loop) ─────────────────────────

    def _source_demoted(self, ws: WebSocket) -> bool:
        """Whether this source has been told to stop pushing (role=backup)."""
        health = self._source_health.get(ws)
        return health is not None and health.get("demoted", False)

    def _touch_source(self, ws: WebSocket) -> None:
        """Record that this source sent a frame, for the silence check.

        Called from the source loop on every accepted frame. Not called for demoted
        sources — they are not expected to send anything once demoted.
        """
        health = self._source_health.get(ws)
        if health is not None and not health.get("demoted", False):
            health["last_frame_at"] = time.time()
            health["body_len_seen"] = len(self.body)

    def _record_frame_health(self, ws: WebSocket, lag_bytes: int = 0,
                             gap: bool = False, mismatch: bool = False) -> None:
        """Accumulate this source's health counters for one BODY frame.

        lag_bytes is how far behind the live edge this source's chunk arrived
        (body_len - offset, 0 for the source that just appended). Demoted sources
        are not tracked — their frames are ignored anyway.
        """
        health = self._source_health.get(ws)
        if health is None or health.get("demoted", False):
            return
        health["lag_bytes"] += max(0, lag_bytes)
        health["frames_seen"] += 1
        if gap:
            health["gap_strikes"] += 1
        if mismatch:
            health["mismatch_strikes"] += 1

    def _source_health_score(self, health: dict) -> float:
        """A single number ordering sources worst-first: higher = worse connection.

        Average lag per frame is the primary signal (it directly measures how far behind
        the live edge this source's identical bytes arrive); gap strikes weight heavily
        since a gappy source is actively dropping data. Used to pick the least-bad
        demoted source for re-promotion.
        """
        frames = max(1, health.get("frames_seen", 0))
        avg_lag = health.get("lag_bytes", 0) / frames
        return avg_lag + 8 * SOURCE_LAG_BYTES * health.get("gap_strikes", 0)

    async def apply_header(self, ws: WebSocket, payload: bytes) -> None:
        """Store canonical header (first received wins). Broadcast once."""
        if self._source_demoted(ws):
            return
        should_broadcast = False
        async with self._lock:
            if not self.header_received:
                self.header[:] = payload
                self.header_received = True
                self.last_active = time.time()
                should_broadcast = True
                log_debug(f"[LIVESTREAMER] [HEADER] Game {self.lobby_id}: stored header ({len(payload)} bytes)")
            elif bytes(self.header) != payload:
                log_warn(f"[LIVESTREAMER] [WARN] HEADER mismatch from another source for game {self.lobby_id}: "
                      f"stored={len(self.header)}B, received={len(payload)}B")
        if should_broadcast:
            await self._broadcast_envelope(MSG_HEADER, payload)
            # The header is what makes a session watchable — before it arrives an observer
            # would connect and sit staring at nothing. Receiving it is therefore the moment
            # the stream becomes live as far as GO is concerned, and this report is what puts
            # the lobby into GO's livestream menu. It goes out now rather than through the
            # batch: until GO has it, /observe turns players away from a working stream.
            await report_stream_live(self.lobby_id)

    async def apply_patch(self, ws: WebSocket, payload: bytes) -> None:
        """Apply patch to header at given offset, broadcast to observers."""
        if self._source_demoted(ws):
            return
        if len(payload) < 8:
            log_warn(f"[LIVESTREAMER] [WARN] PATCH payload too short: {len(payload)} bytes")
            return
        offset = struct.unpack('<I', payload[0:4])[0]
        patch_len = struct.unpack('<I', payload[4:8])[0]
        patch_data = payload[8:8 + patch_len]

        async with self._lock:
            needed = offset + patch_len
            if needed > len(self.header):
                self.header.extend(b'\x00' * (needed - len(self.header)))
            self.header[offset:offset + patch_len] = patch_data
            self.last_active = time.time()
            log_debug(f"[LIVESTREAMER] [PATCH] Game {self.lobby_id}: offset={offset} len={patch_len} header_size={len(self.header)}")
        await self._broadcast_envelope(MSG_PATCH, payload)

    def delayed_watermark(self, now: float) -> int:
        """Body length as of `now - delay`: the newest byte a held observer may receive.

        Binary search over the per-append arrival history. A session younger than the
        delay (or with no recorded history) yields 0 — held observers start from an empty
        file and fill at the delayed edge. Delay 0 means no hold: the watermark is the
        live edge.
        """
        if self.delay_seconds <= 0:
            return len(self.body)
        cutoff = now - self.delay_seconds
        hist = self._body_history
        if not hist or hist[0][0] > cutoff:
            return 0
        if hist[-1][0] <= cutoff:
            return hist[-1][1]
        lo, hi = 0, len(hist) - 1
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if hist[mid][0] <= cutoff:
                lo = mid
            else:
                hi = mid - 1
        return hist[lo][1]

    def _record_body_history(self, ts: float, body_len: int) -> None:
        """Record one (arrival time, body length) pair for the watermark lookup.

        One entry per appended BODY frame. Timestamps are non-decreasing (appends are
        sequential), so the deque doubles as a sorted timeline. Trimmed to a 2x-max-delay
        window plus a hard entry cap, so it stays small even for a long match.
        """
        self._body_history.append((ts, body_len))
        cutoff = ts - 2 * MAX_DELAY_SECONDS
        while len(self._body_history) > 1 and self._body_history[0][0] < cutoff:
            self._body_history.popleft()
        while len(self._body_history) > BODY_HISTORY_MAX:
            self._body_history.popleft()

    async def apply_body(self, ws: WebSocket, payload: bytes) -> None:
        """Append body data. Payload always has [8B offset uint64 LE][data]."""
        if self._source_demoted(ws):
            return
        if len(payload) < 8:
            log_warn(f"[LIVESTREAMER] [WARN] BODY payload too short: {len(payload)} bytes")
            return

        offset = struct.unpack('<Q', payload[0:8])[0]
        data = payload[8:]

        should_broadcast = False
        targets: list = []
        async with self._lock:
            body_len = len(self.body)

            if offset == body_len:
                self.body.extend(data)
                self._record_body_history(time.time(), len(self.body))
                self.last_active = time.time()
                should_broadcast = True
                self._record_frame_health(ws, lag_bytes=0)
                # Fix the recipient list here, while still holding the lock that guards the
                # append. An observer registering after this point records a catch-up limit
                # that already covers these bytes, so sending them the live chunk as well
                # would duplicate it.
                targets = list(self.observer_ws_set)
                if len(self.body) < 5000 or len(self.body) % 50000 == 0:
                    log_debug(f"[LIVESTREAMER] [BODY] Game {self.lobby_id}: +{len(data)}B @ offset={offset} total={len(self.body)}")
            elif offset < body_len:
                overlap = min(len(data), body_len - offset)
                existing = bytes(self.body[offset:offset + overlap])
                if data[:overlap] != existing:
                    log_warn(f"[LIVESTREAMER] [WARN] BODY desync for game {self.lobby_id}: "
                          f"offset={offset} overlap={overlap} mismatch!")
                    self._record_frame_health(ws, lag_bytes=body_len - offset, mismatch=True)
                else:
                    self._record_frame_health(ws, lag_bytes=body_len - offset)
            else:
                log_warn(f"[LIVESTREAMER] [ERROR] BODY gap for game {self.lobby_id}: "
                      f"offset={offset} > body_len={body_len} — dropping, investigate source")
                self._record_frame_health(ws, lag_bytes=offset - body_len, gap=True)

        if should_broadcast:
            file_offset = len(self.header) + offset
            framed = struct.pack('<Q', file_offset) + data
            await self._broadcast_envelope(MSG_BODY, framed, targets=targets)
            # Delay hold: held observers' bytes become available at arrival + delay, so
            # every append is also a chance to advance the shared delayed edge. This is
            # the common delivery path; the global ticker catches quiet moments where no
            # append happens for a while.
            await self._flush_held_observers()

    async def apply_tick(self, ws: WebSocket, payload: bytes) -> None:
        """Frame heartbeat from a source: record it and forward to live-edge observers.

        Carries no game data — only the source's current logic frame — so there is nothing
        to store in the body and nothing to catch up. The value's whole worth is that the
        source sent it *after* flushing that frame's records, which on an ordered transport
        makes it a proof rather than an estimate.
        """
        if self._source_demoted(ws):
            return
        if len(payload) < 4:
            log_warn(f"[LIVESTREAMER] [WARN] TICK payload too short: {len(payload)} bytes")
            return

        frame = struct.unpack('<I', payload[0:4])[0]

        async with self._lock:
            # Monotonic. All-push means several sources forward the same stream, and a source
            # running behind would otherwise pull the advertised edge back — an observer that
            # already simulated to the higher frame cannot un-simulate it.
            if frame <= self.last_tick_frame:
                return
            self.last_tick_frame = frame
            targets = list(self.observer_ws_set)

        await self._broadcast_envelope(MSG_TICK, payload, targets=targets)

    # ── Chat (plans/relay/live-observer-chat.md + live-observer-spectator-chat.md) ──

    async def apply_chat(self, ws: WebSocket, payload: bytes) -> None:
        """Player chat (MSG_CHAT): store + broadcast. Deduped by opaque payload.

        All-push model: every source's client executes the same NetChatCommandMsg at the
        same frame, so several sources forward byte-identical copies of each message —
        whole-payload dedupe against the history drops the duplicates. The frame lives
        inside the payload, so no parsing is needed for the check.
        """
        if self._source_demoted(ws):
            return
        if not payload:
            return
        async with self._lock:
            if payload in self.chat_history:
                return
            self.chat_history.append(payload)
            self.last_active = time.time()
        log_debug(f"[LIVESTREAMER] [CHAT] Game {self.lobby_id}: player chat frame ({len(payload)}B)")
        await self._broadcast_envelope(MSG_CHAT, payload)

    def _source_is_observer(self, ws: WebSocket) -> bool:
        """Whether this source registered as an in-game observer (REGISTER is_observer)."""
        return self._source_observer.get(ws, False)

    async def _send_to_observer_sources(self, payload: bytes) -> None:
        """Fan a spectator chat frame out to observer-mode sources only.

        Streaming players must never see spectator chat, so the receiver set is the
        session's sources filtered by their REGISTER is_observer flag — not all sources.
        Plain send_bytes with per-socket error handling; sources have no catch-up/lock
        machinery (spectator chat is live and unordered).
        """
        frame = pack_frame(MSG_SPECTATOR_CHAT, payload)
        dead: list[WebSocket] = []
        for ws in list(self.sources):
            if not self._source_is_observer(ws):
                continue
            try:
                await ws.send_bytes(frame)
            except Exception as e:
                log_warn(f"[LIVESTREAMER] [WARN] spectator chat send to source failed "
                         f"({type(e).__name__}: {e}), marking dead")
                dead.append(ws)
        for ws in dead:
            self.sources.discard(ws)
            self._source_health.pop(ws, None)
            self._source_observer.pop(ws, None)

    async def apply_spectator_chat(self, ws: WebSocket, payload: bytes) -> None:
        """Spectator chat (MSG_SPECTATOR_CHAT): live, rate-limited, no history.

        Senders: watchers (v1) and, defensively, observer-mode sources. Broadcast to all
        watchers plus observer-mode sources. Deliberately no history/catch-up — a late
        joiner missed it ("you missed it" is the requirement).
        """
        if not payload:
            return
        now = time.time()
        stamps = self._spectator_rate.get(ws, [])
        stamps = [t for t in stamps if now - t < SPECTATOR_RATE_WINDOW]
        if len(stamps) >= SPECTATOR_RATE_MAX:
            log_warn(f"[LIVESTREAMER] [CHAT] spectator chat rate-limited for {id(ws):x}")
            return
        stamps.append(now)
        self._spectator_rate[ws] = stamps
        log_debug(f"[LIVESTREAMER] [CHAT] Game {self.lobby_id}: spectator chat ({len(payload)}B)")
        await self._broadcast_envelope(MSG_SPECTATOR_CHAT, payload)
        await self._send_to_observer_sources(payload)

    def save_replay(self) -> None:
        """Write header + body to a .rep file when the game ends."""
        if not self.header:
            return
        os.makedirs("replays", exist_ok=True)
        filename = f"replays/{self.lobby_id}.rep"
        with open(filename, "wb") as f:
            f.write(bytes(self.header))
            f.write(bytes(self.body))
        log_debug(f"[LIVESTREAMER] [SAVE] Wrote {filename} ({len(self.header)}+{len(self.body)} bytes)")

    # ── Source lifecycle ─────────────────────────────────────────────────

    async def remove_source(self, ws: WebSocket) -> None:
        """Called when a source disconnects. Ends session if all sources gone + END received."""
        should_broadcast_end = False
        should_save = False
        ended_here = False
        async with self._lock:
            self.sources.discard(ws)
            self._source_health.pop(ws, None)
            self._source_observer.pop(ws, None)
            self._spectator_rate.pop(ws, None)
            if not self.sources and self.end_received:
                self.ended = True
                should_broadcast_end = True
                should_save = True
                ended_here = True
                log_debug(f"[LIVESTREAMER] [END] Game {self.lobby_id}: all sources gone, END was received")
            elif not self.sources:
                self.ended = True
                should_save = True
                ended_here = True
                log_debug(f"[LIVESTREAMER] [SOURCE_GONE] Game {self.lobby_id}: last source disconnected"
                      f" ({len(self.sources)} remaining)")
            else:
                log_debug(f"[LIVESTREAMER] [SOURCE_GONE] source disconnected from game {self.lobby_id}... "
                      f"({len(self.sources)} remaining)")
        if should_save:
            self.save_replay()
        if should_broadcast_end:
            self.save_replay()
            await self._broadcast_envelope(MSG_END, b'')
        elif ended_here:
            # Last source gone WITHOUT an END frame: the streamer died or its connection
            # dropped mid-game. That is a disaster, not a game end — tell any observers
            # still watching so they can show "stream lost" in-game instead of waiting
            # out their own watchdog.
            if self.observer_ws_set:
                try:
                    reason_json = json.dumps({"reason": "sources-gone",
                                              "msg": "all streamers disconnected"},
                                             separators=(',', ':'))
                    await self._broadcast_envelope(MSG_ERROR, reason_json.encode())
                except Exception:
                    pass
        # The relay owns closing a stream: when it observes the last source leave (with or
        # without END), it flags the stream ended so the next batch tells GO to stop listing
        # the livestream. This is the only teardown signal — GO has no endpoint to close a
        # stream, because the match ending and the stream ending are different events.
        if ended_here:
            mark_stream_ended(self.lobby_id)
        elif not self.ended:
            # A source left but the session lives on. If it was the last active (non-demoted)
            # pusher, re-promote the least-bad demoted backup so the stream continues instead
            # of stalling on a silent roster.
            await self._maybe_promote_backup()

    # ── All-push demotion / re-promotion (plans/relay/streamer-allpush-demotion.md) ──

    def _active_source_count(self) -> int:
        """Number of sources that are not demoted (i.e. still expected to push)."""
        return sum(1 for ws, h in self._source_health.items()
                   if ws in self.sources and not h.get("demoted", False))

    def _should_demote(self, ws: WebSocket) -> Optional[str]:
        """Return the reason this source should be demoted, or None.

        Checks are cumulative over the source's lifetime (lag_bytes, gap_strikes) rather
        than instantaneous, so a source must *persistently* misbehave before it is demoted —
        one bad frame is jitter, ten are a pattern. A source that is silent while the body
        advances is dead weight and gets demoted too, unless it is the last active pusher
        (that check lives in _maybe_demote_source, which owns the "never demote last" rule).
        """
        health = self._source_health.get(ws)
        if health is None or health.get("demoted", False):
            return None
        if health.get("gap_strikes", 0) >= SOURCE_GAP_STRIKES:
            return f"gap_strikes={health['gap_strikes']} >= {SOURCE_GAP_STRIKES}"
        if health.get("lag_bytes", 0) >= SOURCE_LAG_BYTES:
            return f"lag_bytes={health['lag_bytes']} >= {SOURCE_LAG_BYTES}"
        silent = time.time() - health.get("last_frame_at", 0)
        # Silence only counts as a problem when the body has moved on without this source —
        # a paused game advances nobody's counters, and the streamer of a quiet moment is
        # not misbehaving.
        if silent > SOURCE_SILENCE_SECONDS and len(self.body) > health.get("body_len_seen", 0):
            return f"silent={int(silent)}s"
        return None

    async def _maybe_demote_source(self, ws: WebSocket) -> bool:
        """Demote this source if its health warrants it, never the last active pusher.

        Sends role=backup to the source and marks it demoted so its frames are ignored
        from here on. The last active pusher is exempt: demoting it would leave the session
        with nobody streaming (re-promotion cannot fix a roster of only demoted sources —
        the takeover path needs an active pusher to have left *after* backups were made).
        Returns True if the source was demoted.
        """
        if not ws in self.sources or self._source_demoted(ws):
            return False
        async with self._lock:
            if self._active_source_count() <= 1:
                return False
            reason = self._should_demote(ws)
            if reason is None:
                return False
            self._source_health[ws]["demoted"] = True

        role_json = json.dumps({"role": "backup", "lobbyid": self.lobby_id,
            "body_offset": len(self.body)}, separators=(',', ':'))
        log_warn(f"[LIVESTREAMER] [DEMOTE] Game {self.lobby_id}: source demoted to backup ({reason})")
        try:
            await ws.send_bytes(pack_frame(MSG_ROLE, role_json.encode()))
        except Exception as e:
            log_warn(f"[LIVESTREAMER] [DEMOTE] failed to notify source: {type(e).__name__}: {e}")
        return True

    async def _maybe_promote_backup(self) -> bool:
        """Re-promote the least-bad demoted source when no active pusher remains.

        Called after a source leaves. If demoted backups are still connected, the stream
        must not die: pick the least-bad one (by health score), send it a takeover ROLE
        carrying the current body offset, and let it backfill from its local recording.
        Returns True if a backup was promoted.
        """
        async with self._lock:
            if self._active_source_count() > 0:
                return False
            candidates = [(ws, h) for ws, h in self._source_health.items()
                          if ws in self.sources and h.get("demoted", False)]
            if not candidates:
                return False
            ws, _ = min(candidates, key=lambda item: self._source_health_score(item[1]))
            self._source_health[ws]["demoted"] = False
            body_offset = len(self.body)

        role_json = json.dumps({"role": "streamer", "action": "takeover",
            "lobbyid": self.lobby_id, "body_offset": body_offset}, separators=(',', ':'))
        log_warn(f"[LIVESTREAMER] [PROMOTE] Game {self.lobby_id}: backup promoted to streamer"
              f" at body_offset={body_offset}")
        try:
            await ws.send_bytes(pack_frame(MSG_ROLE, role_json.encode()))
        except Exception as e:
            log_warn(f"[LIVESTREAMER] [PROMOTE] failed to notify backup: {type(e).__name__}: {e}")
            return False
        return True

    # ── Observer lifecycle ───────────────────────────────────────────────

    async def add_observer(self, ws: WebSocket, priority: bool = False) -> Optional[asyncio.Lock]:
        """Register an observer, returning its send lock *already held*.

        The lock is taken before the socket joins observer_ws_set — that is, before it
        becomes a target for _broadcast_envelope. Otherwise a live BODY chunk could be
        delivered ahead of the catch-up chunks that precede it, and since observers write
        each chunk at its absolute file offset, that leaves a hole in the observer's file.
        The old client tolerated it by accident (its playhead ran far behind the tail);
        the parse cursor added for the broadcast delay would stall on it instead.

        priority marks a privileged watcher (admin / user_priority = Viewer, stamped on
        the watch ticket by GO): it bypasses the delay hold and watches the live edge —
        catch-up to the full body, live chunks as they arrive. Everyone else on a delayed
        stream is held: catch-up serves only bytes older than the delay (the watermark at
        registration), and the shared delayed edge delivers the rest at arrival + delay
        (plans/relay/relay-server-side-delay-hold.md).

        The caller MUST release the lock once catch-up has been sent, including on error —
        _broadcast_envelope waits on these locks sequentially, so one held forever would
        block delivery to every observer of this game.
        """
        async with self._lock:
            if len(self.observer_ws_set) >= MAX_OBSERVERS_PER_GAME:
                return None
            send_lock = asyncio.Lock()
            await send_lock.acquire()   # uncontended: nothing else can see it yet
            self._observer_send_locks[ws] = send_lock
            # Held observers start at the delayed edge: their catch-up covers only bytes
            # older than the delay, and the pointer doubles as "delivered so far", so the
            # edge-flush can never duplicate or skip a byte.
            held = not priority and self.delay_seconds > 0
            self._observer_held[ws] = held
            if held:
                self._observer_catchup_limit[ws] = min(len(self.body),
                                                       self.delayed_watermark(time.time()))
                log_debug(f"[OBSERVER] [HOLD] Game {self.lobby_id}: observer held "
                          f"{self.delay_seconds}s behind the live edge")
            else:
                # Body length at the instant this observer became a broadcast target.
                # Catch-up sends up to exactly here and live broadcasts carry on from it,
                # so every byte is delivered exactly once — no hole, no overlapping resend.
                self._observer_catchup_limit[ws] = len(self.body)
            self.observer_ws_set.add(ws)
            self.last_active = time.time()
            mark_observer_change(self.lobby_id)
            return send_lock

    async def remove_observer(self, ws: WebSocket) -> None:
        await self._drop_observer(ws)

    async def _drop_observer(self, ws: WebSocket) -> None:
        """Remove an observer and all of its per-observer state (also the dead-socket sweep)."""
        async with self._lock:
            self.observer_ws_set.discard(ws)
            self._observer_send_locks.pop(ws, None)
            self._observer_catchup_limit.pop(ws, None)
            self._observer_held.pop(ws, None)
            self._spectator_rate.pop(ws, None)
        mark_observer_change(self.lobby_id)

    async def send_catchup(self, ws: WebSocket, last_offset: int = 0,
                           held_lock: Optional[asyncio.Lock] = None) -> None:
        """Send config + header + body[last_offset:] in chunks to a single observer.

        Pass held_lock when the caller already holds this observer's send lock (the normal
        path — see add_observer). Otherwise the lock is acquired here.
        """
        if held_lock is not None:
            await self._send_catchup_locked(ws, last_offset)
            return

        async with self._lock:
            send_lock = self._observer_send_locks.get(ws)
        if send_lock is None:
            return
        async with send_lock:
            await self._send_catchup_locked(ws, last_offset)

    async def _send_catchup_locked(self, ws: WebSocket, last_offset: int) -> None:
        """send_catchup body. Caller must hold this observer's send lock."""
        async with self._lock:
            header_snapshot = bytes(self.header)
            ended_snapshot = self.ended
            held = self._observer_held.get(ws, False)
            tick_snapshot = self.last_tick_frame
            # Held observers get delay_seconds: 0 — the relay's byte-level hold IS the
            # delay, and the client must not double-hold on top of it. Old clients that
            # do not know about the hold play at the held edge correctly either way.
            delay_snapshot = 0 if held else self.delay_seconds
            # Stop exactly where the delivered edge begins. Snapshotting the whole body
            # instead would resend anything appended between registration and now. For a
            # held observer this is the watermark at registration — the delayed edge.
            limit = self._observer_catchup_limit.get(ws, len(self.body))
            body_snapshot = bytes(self.body[:limit])

        # Must precede the HEADER: receiving the header is what starts playback on the
        # observer, and the pre-roll buffer latches against the delay — a value that
        # arrived afterwards would be too late to take effect for this session.
        config_json = json.dumps({"role": "observer", "lobbyid": self.lobby_id,
            "delay_seconds": delay_snapshot}, separators=(',', ':'))
        await ws.send_bytes(pack_frame(MSG_ROLE, config_json.encode()))

        if header_snapshot:
            await ws.send_bytes(pack_frame(MSG_HEADER, header_snapshot))

        last_offset = min(last_offset, len(body_snapshot))
        body_slice = body_snapshot[last_offset:]
        header_size = len(header_snapshot)
        for chunk_off in range(0, len(body_slice), CHUNK_SIZE):
            chunk = body_slice[chunk_off:chunk_off + CHUNK_SIZE]
            chunk_payload = struct.pack('<Q', header_size + last_offset + chunk_off) + chunk
            await ws.send_bytes(pack_frame(MSG_BODY, chunk_payload))

        # Frame heartbeat for the joining observer, after the body it belongs to. Ticks are
        # broadcast, not stored, so without this a joiner would sit on the record-derived
        # edge until the next one arrives. Held observers are excluded for the same reason
        # they are excluded from live ticks (see _broadcast_envelope).
        if tick_snapshot and not held:
            await ws.send_bytes(pack_frame(MSG_TICK, struct.pack('<I', tick_snapshot)))

        if ended_snapshot:
            if held:
                # Stream ended while this observer was joining: nothing is left to spoil,
                # so drain the rest of its body now, then the END frame.
                await self._flush_held_observer_locked(ws, force=True)
            await ws.send_bytes(pack_frame(MSG_END, b''))

        # Player-chat history slice for the joining observer: the last few chat frames,
        # sent after the body. Order is irrelevant — the observer frame-gates them. No
        # spectator-chat history by design.
        chat_slice = list(self.chat_history)[-CHAT_CATCHUP_COUNT:]
        for chat_payload in chat_slice:
            await ws.send_bytes(pack_frame(MSG_CHAT, chat_payload))

        log_debug(f"[OBSERVER] [CATCHUP] Sent header ({len(header_snapshot)}B) + body ({len(body_snapshot)}B, offset={last_offset}) to observer")

    # ── Delay hold (plans/relay/relay-server-side-delay-hold.md) ────────────
    #
    # A held observer never receives BODY bytes directly: the session owns a single
    # delayed edge (the arrival history + watermark), and each held observer only tracks
    # how far it has received (_observer_catchup_limit as a pointer). Delivering the same
    # held copy to every watcher — rather than buffering per observer — is the
    # in-process equivalent of the future dispatcher tier's worker B: a held /watch
    # client of the live worker that re-serves delay_seconds: 0 to its own observers.
    # This design maps onto that one with no new mechanism.

    def _pack_body_frames(self, start: int, end: int) -> list:
        """BODY frames covering body[start:end], chunked, with absolute file offsets."""
        frames = []
        header_size = len(self.header)
        for chunk_off in range(start, end, CHUNK_SIZE):
            chunk_end = min(chunk_off + CHUNK_SIZE, end)
            chunk = bytes(self.body[chunk_off:chunk_end])
            frames.append(pack_frame(MSG_BODY,
                                     struct.pack('<Q', header_size + chunk_off) + chunk))
        return frames

    async def _flush_held_observer(self, ws: WebSocket, now: Optional[float] = None,
                                   force: bool = False) -> None:
        """Deliver a held observer's newly-due body bytes (acquires its send lock)."""
        lock = self._observer_send_locks.get(ws)
        if lock is None:
            return
        async with lock:
            await self._flush_held_observer_locked(ws, now, force)

    async def _flush_held_observer_locked(self, ws: WebSocket, now: Optional[float] = None,
                                          force: bool = False) -> bool:
        """Advance one held observer's delivered edge to the current watermark.

        Caller must hold this observer's send lock — catch-up and the edge flush share
        it, so a held chunk can never overtake the catch-up slice that precedes it.
        force=True ignores the watermark and delivers everything left (stream end:
        nothing is left to spoil). Returns False if the socket failed and the observer
        was dropped.
        """
        if not self._observer_held.get(ws, False):
            return True
        if now is None:
            now = time.time()
        limit = len(self.body) if force else self.delayed_watermark(now)
        pointer = self._observer_catchup_limit.get(ws, 0)
        if limit <= pointer:
            return True
        try:
            for frame in self._pack_body_frames(pointer, limit):
                await ws.send_bytes(frame)
        except Exception as e:
            log_warn(f"[OBSERVER] [WARN] held send to observer failed "
                     f"({type(e).__name__}: {e}), marking dead")
            await self._drop_observer(ws)
            return False
        self._observer_catchup_limit[ws] = limit
        return True

    async def _flush_held_observers(self, now: Optional[float] = None) -> None:
        """Advance every held observer to the shared delayed edge (flush-on-append).

        All held observers of one session share the same watermark, so the edge is
        computed once and each observer just advances its own pointer. Cheap when idle:
        no held observers, no work. The global ticker calls this too, to catch chunks
        whose delay elapsed while no append happened nearby (quiet moments).
        """
        if not self._observer_held:
            return
        if now is None:
            now = time.time()
        for ws in list(self.observer_ws_set):
            if not self._observer_held.get(ws, False):
                continue
            await self._flush_held_observer(ws, now)

    # ── Broadcast ────────────────────────────────────────────────────────

    async def _broadcast_envelope(self, msg_type: int, payload: bytes,
                                  targets: Optional[list] = None) -> None:
        """Send binary frame to observers. Removes dead connections.

        Pass targets to pin the recipient list to a moment in the past — for body data that
        must be the instant the bytes were appended, so an observer that joined afterwards
        (and will receive them via catch-up) is not also sent them live.
        """
        frame = pack_frame(msg_type, payload)
        dead: list[WebSocket] = []

        if msg_type == MSG_END:
            # Stream over: nothing is left to spoil. Held observers get the rest of the
            # body now (force flush), then the END frame below.
            for ws in (targets if targets is not None else list(self.observer_ws_set)):
                if self._observer_held.get(ws, False):
                    await self._flush_held_observer(ws, force=True)

        async def send_one(ws: WebSocket) -> None:
            lock = self._observer_send_locks.get(ws)
            if lock is None:
                return    # already removed, or never fully registered
            # Held observers are served by the shared delayed edge, never a live chunk.
            # Their pointer advances to the watermark at arrival + delay; sending the
            # chunk directly here would put younger-than-delay bytes in their file.
            #
            # MSG_TICK is withheld from them for the same reason one step removed: it
            # carries no bytes, but it advertises the live edge, and the delay hold exists
            # precisely so a modified client cannot know — let alone reach — data younger
            # than the delay. A held observer's edge is the delayed byte edge it is being
            # fed, which is what it already derives from the records themselves.
            if msg_type in (MSG_BODY, MSG_TICK) and self._observer_held.get(ws, False):
                return
            try:
                async with lock:
                    await ws.send_bytes(frame)
            except Exception as e:
                log_warn(f"[OBSERVER] [WARN] send to observer failed ({type(e).__name__}: {e}), marking dead")
                dead.append(ws)

        # Concurrent, not sequential: a single slow/laggy observer must not delay delivery to
        # every other observer of this game. Previously this was a plain `for` loop awaiting
        # each send in turn, which measured as multi-second tail latency once a game had more
        # than ~50-150 concurrent observers (see plans/relay/archive/relay-scaling-rework.md, "Load test
        # findings"). `dead.append` from concurrent tasks is safe without a lock: asyncio tasks
        # are cooperatively scheduled on one thread, so list.append can't interleave.
        #
        # Cap concurrency: one task per observer per BODY chunk means tens of thousands of task
        # creations/sec at scale (4 chunks/s x N games x M observers), which can OOM the relay
        # container. A bounded semaphore keeps the fan-out concurrent but caps the task churn.
        target_list = targets if targets is not None else list(self.observer_ws_set)
        if len(target_list) <= BROADCAST_CONCURRENCY:
            await asyncio.gather(*(send_one(ws) for ws in target_list))
        else:
            sem = asyncio.Semaphore(BROADCAST_CONCURRENCY)

            async def send_one_bounded(ws: WebSocket) -> None:
                async with sem:
                    await send_one(ws)

            await asyncio.gather(*(send_one_bounded(ws) for ws in target_list))

        for ws in dead:
            await self._drop_observer(ws)


# ── In-memory state ────────────────────────────────────────────────────────
games: dict[str, GameSession] = {}

# Single-use credentials minted on GO's behalf via /internal/* (plans/
# relay-go-orchestrated-livestreams.md). In-process for now — becomes Redis once the
# dispatcher tier in plans/relay/archive/relay-scaling-rework.md exists and needs the same lookup shared
# across processes. Key -> {lobby_id, user_id, expires_at} (expires_at unix seconds).
watch_tickets: dict[str, dict] = {}
stream_tokens: dict[str, dict] = {}


def _new_credential(lobby_id: str, user_id, store: dict, priority: bool = False) -> str:
    key = secrets.token_urlsafe(24)
    store[key] = {
        "lobby_id": lobby_id,
        "user_id": user_id,
        # Priority watchers (admin / user_priority = Viewer, decided by GO at mint time)
        # bypass the relay's byte-level delay hold. GO stamps it on watch tickets; stream
        # tokens never carry it (sources are not observers), so an absent value is False.
        "priority": bool(priority),
        # The fixed short lifetime applies to every credential: the client flow is atomic
        # (mint -> send -> connect), so a short TTL is enough and keeps the replay window small.
        "expires_at": time.time() + WATCH_TICKET_TTL_SECONDS,
    }
    return key


def _consume_credential(key: Optional[str], lobby_id: str, store: dict) -> Optional[dict]:
    """Validate and burn a single-use credential. Returns the dict on success, else None.

    Pops unconditionally (even on a lobby_id mismatch) so a credential is single-use regardless
    of which check fails it — a client can't retry a stolen/mismatched credential against a
    different lobby_id after a first failed attempt.
    """
    if not key:
        return None
    cred = store.pop(key, None)
    if cred is None:
        return None
    if cred["lobby_id"] != lobby_id or cred["expires_at"] < time.time():
        return None
    return cred


def consume_watch_ticket(key: Optional[str], lobby_id: str) -> Optional[dict]:
    return _consume_credential(key, lobby_id, watch_tickets)


def consume_stream_token(key: Optional[str], lobby_id: str) -> Optional[dict]:
    return _consume_credential(key, lobby_id, stream_tokens)


def _get_go_notify_session() -> aiohttp.ClientSession:
    global _go_notify_session
    if _go_notify_session is None or _go_notify_session.closed:
        _go_notify_session = aiohttp.ClientSession()
    return _go_notify_session


# Lobbies whose GO-visible state changed since the last report, split by what changed:
#   _observer_dirty: the observer count changed (report current count, is_live=True)
#   _ended_dirty:    the stream closed (report is_live=False, count 0)
# A single shared timer drains both into one batched request to GO. The state is a rough
# estimate — never per-event liveness — so batching every change since the last send is correct.
_observer_dirty: set[str] = set()
_ended_dirty: set[str] = set()
_observer_batch_task: Optional[asyncio.Task] = None


def _arm_observer_batch() -> None:
    """Start the shared batch timer if one is not already pending (never reset it)."""
    global _observer_batch_task
    if _observer_batch_task is not None and not _observer_batch_task.done():
        return

    async def _flush_later() -> None:
        try:
            await asyncio.sleep(OBSERVER_CHANGE_TIMEOUT)
        except asyncio.CancelledError:
            return
        global _observer_batch_task
        _observer_batch_task = None
        await flush_observer_batch()

    _observer_batch_task = asyncio.create_task(_flush_later())


async def report_stream_live(lobby_id: str) -> None:
    """Tell GO a stream became watchable, without waiting for the batch window.

    Observer counts and stream endings are rough estimates, and coalescing them costs nothing
    that matters. A stream *starting* is not in that category: GO refuses to admit an observer
    until it knows the stream is live, so every second spent batching here is a second the game
    sits in the menu — or fails to appear in it — while being perfectly watchable.

    Falls back to the batch when the POST does not land, so a stream that GO missed is still
    retried rather than silently never becoming visible. No-op if GO reporting is disabled.
    """
    if not GO_OBSERVERS_URL:
        return

    session = games.get(lobby_id)
    # Same rule the batch applies: no header means nothing to watch yet, so not live.
    if session is None or session.ended or not session.header_received:
        return

    count = len(session.observer_ws_set)
    entries = [{"lobby_id": str(lobby_id), "observer_count": count, "is_live": True}]

    if await notify_lobby_progress(entries):
        session._last_reported_observers = count
        return

    log_warn(f"[LIVESTREAM] [WARN] immediate live report for {lobby_id} failed; falling back "
             f"to the batch")
    _observer_dirty.add(lobby_id)
    _arm_observer_batch()


def mark_observer_change(lobby_id: str) -> None:
    """Record that a lobby's observer set changed and arm the shared batch timer.

    Called on every observer join/leave (including the dead-socket sweep). The first change
    arms a single OBSERVER_CHANGE_TIMEOUT timer; changes that arrive before it fires are added
    to the same batch, and the timer is never reset — the batch always goes out
    OBSERVER_CHANGE_TIMEOUT seconds after the first change. No-op if GO reporting is disabled.

    Counts only. A stream becoming watchable goes out immediately instead — see
    report_stream_live.
    """
    if not GO_OBSERVERS_URL:
        return
    _observer_dirty.add(lobby_id)
    _arm_observer_batch()


def mark_stream_ended(lobby_id: str) -> None:
    """Record that a lobby's stream closed and arm the shared batch timer.

    The relay owns stream liveness (it observes the last source leave / END / inactivity
    reaping), so when it closes a session it flags the lobby for an is_live=False report
    rather than maintaining a separate ended notification path. No-op if GO reporting is
    disabled.
    """
    if not GO_OBSERVERS_URL:
        return
    _ended_dirty.add(lobby_id)
    _observer_dirty.discard(lobby_id)
    _arm_observer_batch()


async def flush_observer_batch() -> None:
    """Post every dirty lobby's livestream state to GO in a single request.

    Ended lobbies are reported with is_live=False and a count of 0. Live lobbies are reported
    with their current count at flush time (so all changes since the last send are reflected)
    and is_live=True; those whose count is unchanged from the last posted value are skipped.
    No-op when nothing is dirty or reporting is disabled.

    Nothing is committed until GO has accepted the batch. A lobby stays dirty and
    _last_reported_observers stays where it was until the POST succeeds, because a dropped
    is_live=False would otherwise strand a dead stream in GO's livestream menu permanently,
    and a dropped count would sit stale until the count happened to change again.
    """
    if not GO_OBSERVERS_URL or (not _observer_dirty and not _ended_dirty):
        return

    entries = []

    ended_batch = list(_ended_dirty)
    for lobby_id in ended_batch:
        entries.append({"lobby_id": str(lobby_id), "observer_count": 0, "is_live": False})

    observer_batch = []
    for lobby_id in list(_observer_dirty):
        session = games.get(lobby_id)
        # A session with no header yet is not watchable, so it is not live: reporting it would
        # put a lobby in GO's menu that an observer can only stare at. apply_header marks the
        # lobby dirty again the moment that changes.
        if session is None or session.ended or not session.header_received:
            _observer_dirty.discard(lobby_id)
            continue
        count = len(session.observer_ws_set)
        if count == session._last_reported_observers:
            _observer_dirty.discard(lobby_id)
            continue
        observer_batch.append((lobby_id, count))
        entries.append({"lobby_id": str(lobby_id), "observer_count": count, "is_live": True})

    if not entries:
        return

    if not await notify_lobby_progress(entries):
        # Leave both dirty sets exactly as they are and try again on the next tick, rather
        # than dropping state GO never received.
        _arm_observer_batch()
        return

    for lobby_id in ended_batch:
        _ended_dirty.discard(lobby_id)

    for lobby_id, count in observer_batch:
        session = games.get(lobby_id)
        if session is not None:
            session._last_reported_observers = count
        _observer_dirty.discard(lobby_id)


async def notify_lobby_progress(entries: list) -> bool:
    """Tell GO services the current livestream state for a set of lobbies.

    Returns True only when GO accepted the batch. Never raises: GO being down or unreachable
    must not affect the relay's own streaming, so a failure is reported back as False and the
    caller retries. Authenticates with the relay's own key (GO_API_KEY) as X-Relay-Key, which
    GO validates against Relay.ingress_api_key.
    """
    if not GO_OBSERVERS_URL or not entries:
        return False
    try:
        async with _get_go_notify_session().post(
            GO_OBSERVERS_URL,
            json=entries,
            headers={"X-Relay-Key": GO_API_KEY} if GO_API_KEY else {},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as response:
            if response.status >= 300:
                log_warn(f"[LIVESTREAM] [WARN] GO rejected livestream state with HTTP "
                         f"{response.status}; will retry")
                return False
        log_debug(f"[LIVESTREAM] notified GO livestream state: {entries}")
        return True
    except Exception as e:
        log_warn(f"[LIVESTREAM] [WARN] failed to notify GO livestream state: {type(e).__name__}: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════
# Internal endpoints (GO services only)
# ═══════════════════════════════════════════════════════════════════════════

def _check_internal_key(request: Request) -> bool:
    """Constant-time check of the X-Relay-Key header against the configured secret."""
    if not INTERNAL_API_KEY:
        return False
    supplied = request.headers.get("x-relay-key", "")
    return hmac.compare_digest(supplied, INTERNAL_API_KEY)


def _require_internal_key(request: Request) -> None:
    if not INTERNAL_API_KEY:
        raise HTTPException(status_code=503, detail="relay not configured with INTERNAL_API_KEY")
    if not _check_internal_key(request):
        raise HTTPException(status_code=401, detail="invalid or missing relay key")


def _public_host(request: Request) -> str:
    """Scheme/host used when building public URLs handed to clients."""
    return PUBLIC_HOST or (request.headers.get("host") or request.url.hostname)


def _public_path_prefix() -> str:
    """Normalized path prefix for public connect URLs (e.g. '/relay' or '')."""
    prefix = PUBLIC_PATH_PREFIX.strip()
    if not prefix:
        return ""
    if not prefix.startswith("/"):
        prefix = "/" + prefix
    return prefix.rstrip("/")


def _public_base(request: Request, lobby_id: str) -> str:
    return f"{PUBLIC_WS_SCHEME}://{_public_host(request)}{_public_path_prefix()}/stream/{lobby_id}"


def _public_ws_url(request: Request, url_path: str, lobby_id: str, query_param: str, key: str) -> str:
    return f"{PUBLIC_WS_SCHEME}://{_public_host(request)}{_public_path_prefix()}/{url_path}/{lobby_id}?{query_param}={key}"


async def _internal_payload(request: Request) -> dict:
    """Authenticate an /internal/* call and return its JSON body."""
    _require_internal_key(request)
    return await request.json()


def _require_lobby_id(body: dict) -> str:
    """Extract a required lobby_id from an /internal/* body; 400 if absent."""
    lobby_id = str(body.get("lobby_id", ""))
    if not lobby_id:
        raise HTTPException(status_code=400, detail="lobby_id required")
    return lobby_id


def _require_live_session(lobby_id: str) -> None:
    """404 unless a live (not ended) session exists for this lobby.

    The detail carries a machine-readable `code` so GO can tell this apart from a bare routing
    404 (wrong Relay.base_url, a reverse proxy that mis-handled the path prefix). Both are 404s
    on the wire, but only this one means the stream is over, and only this one should be shown
    to a player as "that livestream has ended".
    """
    session = games.get(lobby_id)
    if not session or session.ended:
        raise HTTPException(status_code=404,
                            detail={"code": "stream_ended", "message": "game not found or ended"})


def _get_or_create_session(lobby_id: str) -> GameSession:
    """Return the live session for a lobby, creating a fresh one if none exists yet."""
    session = games.get(lobby_id)
    if session is None or session.ended:
        session = GameSession(lobby_id)
        games[lobby_id] = session
    return session


def _mint_credential(request: Request, lobby_id: str, body: dict,
                     store: dict, url_path: str, query_param: str) -> dict:
    """Mint a single-use credential and return its public connect URL."""
    user_id = body.get("user_id")
    priority = bool(body.get("priority", False))
    key = _new_credential(lobby_id, user_id, store, priority=priority)
    log_debug(f"[TICKET] [INTERNAL] {query_param} minted for user_id={user_id} "
              f"lobby={lobby_id} priority={priority}")
    return {"url": _public_ws_url(request, url_path, lobby_id, query_param, key)}


async def _mint_credential_for_lobby(request: Request, store: dict,
                                     url_path: str, query_param: str) -> dict:
    """Shared handler for /internal/stream_tokens and /internal/watch_tickets."""
    body = await _internal_payload(request)
    lobby_id = _require_lobby_id(body)
    _require_live_session(lobby_id)
    return _mint_credential(request, lobby_id, body, store, url_path, query_param)


@app.post("/internal/livestreams")
async def internal_create_livestream(request: Request):
    """GO announces a new livestream for an in-game lobby. Creates the relay session."""
    body = await _internal_payload(request)
    lobby_id = _require_lobby_id(body)

    session = _get_or_create_session(lobby_id)
    session.owner_user_id = body.get("owner_user_id")

    # The broadcast delay the host chose, forwarded by GO. Only the host's registration carries
    # one (GO sends null for every other member registering their own source), so an absent
    # value must leave an already-established delay alone rather than reset it.
    raw_delay = body.get("delay_seconds")
    if raw_delay is not None:
        delay = parse_delay_seconds(raw_delay)
        if delay is None:
            log_warn(f"[LIVESTREAMER] [WARN] GO sent bad delay_seconds={raw_delay!r} for "
                     f"lobby={lobby_id}, keeping {session.delay_seconds}")
        else:
            session.delay_seconds = delay
            session.delay_from_go = True
            log_debug(f"[LIVESTREAMER] [DELAY] Game {lobby_id}: delay_seconds={delay} (from GO)")

    log_debug(f"[TICKET] [INTERNAL] livestream registered for lobby={lobby_id}")
    return {"base_url": _public_base(request, lobby_id)}


@app.post("/internal/stream_tokens")
async def internal_create_stream_token(request: Request):
    """Mint a single-use stream token for one lobby member (a streamer)."""
    return await _mint_credential_for_lobby(request, stream_tokens, "stream", "stream_token")


@app.post("/internal/watch_tickets")
async def internal_create_watch_ticket(request: Request):
    """Mint a single-use watch ticket for one observer of a livestream."""
    return await _mint_credential_for_lobby(request, watch_tickets, "watch", "ticket")


# There is deliberately no DELETE /internal/livestreams/{lobby_id}. The relay owns closing a
# stream: it is the only party that can see the last source leave, an END frame arrive, or a
# session go quiet, and it reports that to GO as is_live=False. GO ending the match is not the
# same event as the stream ending (a match can run on with nobody streaming it), so a GO-driven
# teardown would only add a second, racier path to the same state.


# ═══════════════════════════════════════════════════════════════════════════
# REST endpoints
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/health")
async def health():
    total_observers = sum(len(g.observer_ws_set) for g in games.values())
    total_body_bytes = sum(len(g.body) for g in games.values())
    return {
        "status": "ok",
        "active_games": sum(1 for g in games.values() if not g.ended),
        "total_observers": total_observers,
        "total_body_bytes": total_body_bytes,
    }


# ═══════════════════════════════════════════════════════════════════════════
# WebSocket /stream/{lobby_id} (sources)
# ═══════════════════════════════════════════════════════════════════════════

@app.websocket("/stream/{lobby_id}")
async def stream_endpoint(websocket: WebSocket, lobby_id: str):
    """
    A streamer connects here with the single-use stream_token GO minted for them.

    Protocol (binary):
    1. Client sends REGISTER frame (type=0), payload = JSON with lobbyid/can_stream/player_name
    2. Server sends ROLE frame (type=5), payload = JSON {"role":"streamer","lobbyid":"..."}
    3. Source sends HEADER (type=1), then PATCH/BODY/END (type=2/3/4)
    """
    await websocket.accept()
    session: Optional[GameSession] = None
    role: str = "unknown"

    token = websocket.query_params.get("stream_token")
    credential = consume_stream_token(token, lobby_id)
    if credential is None:
        await reject(websocket, "Invalid or expired stream token")
        return

    try:
        # ── Receive REGISTER frame (binary) ────────────────────────────
        msg = await websocket.receive()
        if "bytes" not in msg:
            await reject(websocket, "Expected binary REGISTER frame")
            return

        raw_bytes = msg["bytes"]
        log_debug(f"[LIVESTREAMER] [REGISTER_RAW] {len(raw_bytes)} bytes: {raw_bytes[:80].hex()} ...")
        msg_type, payload = unpack_frame(raw_bytes)
        if msg_type != MSG_REGISTER or not payload:
            await reject(websocket, "Expected REGISTER message (type=0)")
            return

        reg_text = payload.decode("utf-8", errors="replace")
        log_debug(f"[LIVESTREAMER] [REGISTER] received: {repr(reg_text[:200])}")
        reg = json.loads(reg_text)

        player_name = reg.get("player_name", "unknown")
        can_stream = reg.get("can_stream", False)
        # In-game observer (Side 1, not an active player)? Gates which sources receive
        # spectator chat (plans/relay/live-observer-spectator-chat.md).
        is_observer = bool(reg.get("is_observer", False))

        if not can_stream:
            await reject(websocket, "stream token valid, but can_stream required")
            return

        # ── Assign session ─────────────────────────────────────────────
        # The session is keyed by the URL lobby_id, which is GO's LobbyID (the value GO
        # minted the stream token for). A REGISTER payload carrying a different lobbyid is
        # ignored — the URL is the authority here.
        session = _get_or_create_session(lobby_id)

        # ── Host-authoritative fields ──────────────────────────────────
        # Host authority is GO's to grant, not the client's to claim. GO records the lobby's
        # owner on the session and mints each stream token against a specific user id, so the
        # two are compared here instead of trusting an is_host flag in the REGISTER payload —
        # otherwise any member could relabel the game every other member is streaming.
        is_host = (session.owner_user_id is not None and
                   credential.get("user_id") == session.owner_user_id)

        if is_host:
            lobby = sanitize_lobby(reg.get("lobby"))
            if lobby:
                session.lobby = lobby
                log_debug(f"[LIVESTREAMER] [REGISTER] host described lobby "
                          f"'{lobby.get('name', '')}' on '{lobby.get('mapname', '')}' "
                          f"({len(lobby_player_names(lobby))} players) for {session.lobby_id}")

            # Fallback only: GO forwards the host's delay on /internal/livestreams before any
            # source connects, and that value wins. This path covers a GO that does not send
            # one (older services build), so it must not undo what GO already established.
            raw_delay = reg.get("delay_seconds")
            if raw_delay is not None and not session.delay_from_go:
                delay = parse_delay_seconds(raw_delay)
                if delay is None:
                    log_warn(f"[LIVESTREAMER] [WARN] bad delay_seconds={raw_delay!r}, "
                          f"keeping {session.delay_seconds}")
                else:
                    session.delay_seconds = delay
                    log_debug(f"[LIVESTREAMER] [DELAY] Game {session.lobby_id}: "
                          f"delay_seconds={session.delay_seconds} (from REGISTER)")

        role = "streamer"
        async with session._lock:
            session.sources.add(websocket)
            session._source_observer[websocket] = is_observer
            session._source_health[websocket] = {
                "demoted": False,
                "lag_bytes": 0,
                "gap_strikes": 0,
                "mismatch_strikes": 0,
                "frames_seen": 0,
                "last_frame_at": time.time(),
                "body_len_seen": len(session.body),
            }

        # ── Send ROLE response (binary) ────────────────────────────────
        role_json = json.dumps({"role": role, "lobbyid": session.lobby_id,
            "body_offset": len(session.body)}, separators=(',', ':'))
        await websocket.send_bytes(pack_frame(MSG_ROLE, role_json.encode()))
        log_debug(f"[LIVESTREAMER] [REGISTER] {player_name} -> role={role} game={session.lobby_id}...")

        await _source_loop(websocket, session)

    except WebSocketDisconnect:
        log_debug(f"[LIVESTREAMER] [DISCONNECT] Client disconnected (role={role})")
    except Exception as e:
        log_warn(f"[LIVESTREAMER] [ERROR] /stream error: {e}")
        try:
            await websocket.send_bytes(pack_frame(MSG_ERROR, f"Internal error: {e}".encode()))
        except Exception:
            pass
    finally:
        if session and role == "streamer":
            await session.remove_source(websocket)


async def _source_loop(ws: WebSocket, session: GameSession) -> None:
    """Receive binary frames (HEADER/PATCH/BODY/END) from a source."""
    while True:
        msg = await ws.receive()
        if msg.get("type") == "websocket.disconnect":
            break
        if "text" in msg:
            continue
        if "bytes" not in msg:
            continue

        raw = msg["bytes"]
        msg_type, payload = unpack_frame(raw)
        if msg_type is None:
            continue

        if msg_type == MSG_HEADER:
            await session.apply_header(ws, payload)
        elif msg_type == MSG_PATCH:
            await session.apply_patch(ws, payload)
        elif msg_type == MSG_BODY:
            await session.apply_body(ws, payload)
        elif msg_type == MSG_TICK:
            await session.apply_tick(ws, payload)
        elif msg_type == MSG_CHAT:
            await session.apply_chat(ws, payload)
        elif msg_type == MSG_SPECTATOR_CHAT:
            # Defence in depth: a streaming player may not send spectator chat; an
            # observer-mode source may (the client does not send it in v1, but the relay
            # should not reject a legitimate observer-source).
            if session._source_is_observer(ws):
                await session.apply_spectator_chat(ws, payload)
        elif msg_type == MSG_END:
            async with session._lock:
                session.end_received = True
            log_debug(f"[LIVESTREAMER] [END] Source sent END for game {session.lobby_id}")
            break

        session._touch_source(ws)
        # Demotion is checked per-frame so a persistently bad source is stopped quickly.
        # The per-source counters make this cheap: no timers, just arithmetic.
        if await session._maybe_demote_source(ws):
            log_debug(f"[LIVESTREAMER] [DEMOTE] Source {id(ws):x} no longer pushing for "
                      f"game {session.lobby_id}")


# ═══════════════════════════════════════════════════════════════════════════
# WebSocket /watch/{lobby_id} (observers)
# ═══════════════════════════════════════════════════════════════════════════

@app.websocket("/watch/{lobby_id}")
async def watch_game(websocket: WebSocket, lobby_id: str):
    """
    An observer connects here with the single-use watch ticket GO minted for them.

    Protocol (binary):
    1. Server sends HEADER (type=1) + BODY chunks (type=3) for catch-up
    2. Server streams live PATCH/BODY/END (type=2/3/4)
    """
    await websocket.accept()

    # Consume the ticket before the session check, matching /stream's ordering: a single-use
    # credential is burned on first use regardless of what check rejects it, so a client can't
    # retry a ticket against a different lobby or an ended game.
    ticket = websocket.query_params.get("ticket")
    credential = consume_watch_ticket(ticket, lobby_id)
    if credential is None:
        await reject(websocket, "Missing or invalid watch ticket")
        return

    session = games.get(lobby_id)
    if not session or session.ended:
        await reject(websocket, "Game not found or ended")
        return

    # GO stamps priority on the ticket for privileged watchers (admin / user_priority =
    # Viewer): they bypass the byte-level delay hold and watch the live edge. Everyone
    # else on a delayed stream is held (plans/relay/relay-server-side-delay-hold.md).
    send_lock = await session.add_observer(websocket,
                                           priority=bool(credential.get("priority", False)))
    if send_lock is None:
        await reject(websocket, "Max observers reached")
        return

    log_debug(f"[OBSERVER] [WATCH] Observer connected to game {lobby_id} ({len(session.observer_ws_set)} viewers)")

    try:
        # add_observer handed us the send lock already held, so live broadcasts queue
        # behind catch-up instead of racing ahead of it. Release it no matter what.
        try:
            await session.send_catchup(websocket, last_offset=0, held_lock=send_lock)
        finally:
            send_lock.release()

        while True:
            msg = await websocket.receive()
            if msg.get("type") == "websocket.disconnect":
                break

            # Watchers are senders of spectator chat (MSG_SPECTATOR_CHAT). Anything else
            # from an observer is ignored — observers push nothing else.
            if "bytes" in msg:
                try:
                    msg_type, payload = unpack_frame(msg["bytes"])
                    if msg_type == MSG_SPECTATOR_CHAT:
                        await session.apply_spectator_chat(websocket, payload)
                except Exception:
                    pass

    except WebSocketDisconnect:
        log_debug(f"[OBSERVER] [WATCH] Observer disconnected from game {lobby_id}")
    except Exception as e:
        log_debug(f"[OBSERVER] [WATCH] Observer error: {e}")
    finally:
        await session.remove_observer(websocket)


# ═══════════════════════════════════════════════════════════════════════════
# Background cleanup
# ═══════════════════════════════════════════════════════════════════════════

async def _cleanup_loop():
    """Periodically remove ended/inactive/undescribed games; purge expired credentials."""
    while True:
        await asyncio.sleep(15)
        now = time.time()

        for store in (watch_tickets, stream_tokens):
            expired = [k for k, c in store.items() if c["expires_at"] < now]
            for k in expired:
                store.pop(k, None)

        # Zombie-source probe: the rules above never reap a session with connected sources,
        # so a source whose TCP connection died without a disconnect event would keep its
        # session alive forever. Probe silent sources and drop the dead ones — a live but
        # idle streamer (a stalled game) answers the ping fine and stays.
        for session in list(games.values()):
            if session.ended or not session.sources:
                continue
            if now - session.last_active <= INACTIVE_GAME_TTL:
                continue
            for ws in list(session.sources):
                try:
                    await asyncio.wait_for(ws.ping(), timeout=5)
                except Exception:
                    log_warn(f"[LIVESTREAMER] [CLEANUP] Game {session.lobby_id}: source "
                             f"{id(ws):x} silent for {int(now - session.last_active)}s and "
                             f"unresponsive to ping — removing it")
                    await session.remove_source(ws)

        to_remove = []
        notify_reasons = {}
        for lobby_id, session in games.items():
            # A session with connected sources is alive even when idle: the game may be
            # paused/stalled (the streamer's socket is open and it believes it is
            # streaming), and reaping it would kill the watch for every observer while the
            # streamer keeps uploading into a dead session — the "UI says streaming but
            # /health says no stream" failure. Inactivity only reaps sessions nobody is
            # connected to anymore (a source that vanished without a disconnect event, or
            # a session that was created but never claimed).
            idle_without_sources = (not session.sources
                                    and now - session.last_active > INACTIVE_GAME_TTL)
            if session.ended or idle_without_sources:
                to_remove.append(lobby_id)
                # A session already marked `ended` was closed by remove_source, which already
                # notified GO. A session reaped purely for inactivity was never closed by a
                # source disconnect, so the relay must report it here.
                if not session.ended:
                    notify_reasons[lobby_id] = "inactivity"
                continue

        # A session nobody ever described as host (the host's REGISTER carries the lobby
        # block) is dropped once it is clearly abandoned — but only while no source is
        # connected. A non-host source streaming without the host is alive and watchable
        # (the is_live report fires on any header), so reaping it at 120s just because the
        # host chose not to stream would kill a working watch — the streamers would keep
        # "streaming" into a dead session with /health showing nothing.
        if not session.lobby and not session.sources \
                and (now - session.created_at > UNDESCRIBED_GAME_TTL):
            log_warn(f"[LIVESTREAMER] [CLEANUP] No host registration for {lobby_id} after "
                     f"{UNDESCRIBED_GAME_TTL}s; dropping (host not streaming?)")
            to_remove.append(lobby_id)
            notify_reasons[lobby_id] = "undescribed"

        for lobby_id in to_remove:
            session = games.pop(lobby_id, None)
            if session:
                reason = notify_reasons.get(lobby_id)
                # If sources are still connected when the session goes away (defensive —
                # the inactivity rule above normally prevents this), they would keep
                # uploading into a reaped session forever and show "streaming" with no
                # stream anywhere. Tell them, loudly, and close their sockets so the
                # client winds down instead of hanging.
                if session.sources:
                    log_warn(f"[LIVESTREAMER] [CLEANUP] Game {lobby_id} removed with "
                             f"{len(session.sources)} source(s) still connected"
                             f" (reason={reason or 'ended'}) — notifying and closing them")
                    for ws in list(session.sources):
                        try:
                            reason_json = json.dumps({"reason": reason or "session_ended"},
                                                     separators=(',', ':'))
                            await ws.send_bytes(pack_frame(MSG_ERROR, reason_json.encode()))
                        except Exception:
                            pass
                        try:
                            await ws.close()
                        except Exception:
                            pass
                # A reap is a disaster, not a game end: the streamer never sent END. Tell
                # any observers still attached so they can show "stream lost" in-game
                # instead of finishing as if the match had ended normally.
                if reason and session.observer_ws_set:
                    try:
                        reason_json = json.dumps(
                            {"reason": reason, "msg": "relay ended the session"},
                            separators=(',', ':'))
                        await session._broadcast_envelope(MSG_ERROR, reason_json.encode())
                    except Exception:
                        pass
                try:
                    await session._broadcast_envelope(MSG_END, b'')
                except Exception:
                    pass
                log_warn(f"[LIVESTREAMER] [CLEANUP] Removed game {lobby_id}"
                         f" (reason={reason or 'ended'}, sources={len(session.sources)},"
                         f" observers={len(session.observer_ws_set)},"
                         f" body={len(session.body)}B)")
                # A session already marked `ended` was closed by remove_source, which already
                # flagged it for an is_live=False report. A session reaped purely for
                # inactivity/undescribed was never closed by a source disconnect, so the relay
                # must flag it here.
                if reason:
                    mark_stream_ended(lobby_id)

        # Silence sweep for the all-push demotion model: a source that is silent while the
        # body advances is dead weight and should be demoted. This runs here because the
        # per-frame check in _source_loop only fires when *some* source is still sending —
        # if every source goes quiet, nobody triggers it. The "never demote the last active
        # pusher" guard inside _maybe_demote_source keeps the stream alive regardless.
        for session in games.values():
            if session.ended:
                continue
            for ws in list(session.sources):
                await session._maybe_demote_source(ws)


async def _delay_flush_loop():
    """Deliver held observers' due body bytes on a fixed cadence.

    Flush-on-append covers the common case; this catches chunks whose delay elapsed while
    no body arrived nearby (quiet moments, appends stalled). Cheap when idle: no held
    observers, no work.
    """
    while True:
        await asyncio.sleep(DELAY_FLUSH_INTERVAL)
        for session in list(games.values()):
            if session.ended or not session._observer_held:
                continue
            try:
                await session._flush_held_observers()
            except Exception as e:
                log_warn(f"[OBSERVER] [WARN] delay flush failed for game "
                         f"{session.lobby_id}: {type(e).__name__}: {e}")


async def _observer_report_loop():
    """Periodically flush the observer-count batch to GO.

    A baseline on top of the change-triggered flush: even a stream whose observer set has been
    static for a while gets a fresh state every OBSERVER_UPDATE_INTERVAL, so GO's observer_count
    never goes stale (e.g. if a change-triggered flush was lost). It drains the same dirty sets,
    so lobbies whose count is unchanged from the last posted value are skipped — cheap.
    """
    while True:
        await asyncio.sleep(OBSERVER_UPDATE_INTERVAL)
        try:
            await flush_observer_batch()
        except Exception as e:
            log_warn(f"[LIVESTREAM] [WARN] periodic observer batch flush failed: {type(e).__name__}: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# Startup / main
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    host = os.getenv("HOST", "0.0.0.0")
    print(f"[START] cc-live-relay v0.7.0 starting on {host}:{PORT}")
    print(f"[START] Max observers: {MAX_OBSERVERS_PER_GAME}, Chunk size: {CHUNK_SIZE} bytes")
    if not INTERNAL_API_KEY:
        print("[START] WARNING: INTERNAL_API_KEY is not set — /internal/* endpoints will refuse all calls")
    if not GO_OBSERVERS_URL:
        print("[START] WARNING: GO_OBSERVERS_URL is not set — GO is told nothing about livestream "
              "state, so no stream will ever appear in its livestream menu")
    if GO_OBSERVERS_URL and not GO_API_KEY:
        print("[START] WARNING: GO_OBSERVERS_URL is set but GO_API_KEY is not — "
              "GO will reject the observer-count notification (401)")
    uvicorn.run(app, host=host, port=PORT)
