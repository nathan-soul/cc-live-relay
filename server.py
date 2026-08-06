"""
cc-live-relay — Live game relay server for Generals Zero Hour

Architecture: GO Services → Relay → Observer/Streamer
GO services validates user JWTs and calls this relay over HTTP with a shared
INTERNAL_API_KEY to mint single-use stream tokens (streamers) and watch tickets
(observers). This relay never sees a user JWT; it only trusts GO. See
plans/relay-go-orchestrated-livestreams.md for the full design.

WebSocket-based relay with binary envelope protocol (msg types 0-6).
Aligned with the C++ LiveStreamer/LiveObserver client (libcurl websockets).
"""

import asyncio
import hmac
import json
import os
import secrets
import struct
import time
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

CHUNK_SIZE = 256 * 1024  # 256 KB per chunk for observer catch-up

# ── Configuration via environment variables ────────────────────────────────
PORT = int(os.getenv("PORT", "8765"))
MAX_OBSERVERS_PER_GAME = int(os.getenv("MAX_OBSERVERS_PER_GAME", "1000"))
INACTIVE_GAME_TTL = 60
# How long a session may exist without the host ever describing it before it is dropped. Only
# the host's REGISTER carries the lobby block, so until it arrives the game cannot be listed or
# meaningfully watched — see _cleanup_loop.
UNDESCRIBED_GAME_TTL = int(os.getenv("UNDESCRIBED_GAME_TTL", "120"))

# Broadcast delay: how far behind live an observer is held. The streamer owns this value
# (it is their spoiler window), sends it in REGISTER, and the relay forwards it to every
# observer before any replay data. Used when a streamer sends nothing, or runs an older
# build that does not know about the field.
DEFAULT_DELAY_SECONDS = int(os.getenv("DEFAULT_DELAY_SECONDS", "15"))
MAX_DELAY_SECONDS = 600

# Verbose per-game / per-connection logging. Enable with DEBUG=1 (or "true"/"yes"/"on").
DEBUG = os.getenv("DEBUG", "").strip().lower() in ("1", "true", "yes", "on")

# ── GO-orchestration config (plans/relay-go-orchestrated-livestreams.md) ────
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

# Where GO services receives the relay's "this livestream ended" notification. The relay
# owns closing a stream (it observes the last source leave / END / inactivity reaping), so
# when it closes a session it reports back here rather than waiting for GO to ask. Optional:
# empty means the relay runs without notifying GO (the session still closes either way).
# The relay's own credential (GO_API_KEY, distinct from INTERNAL_API_KEY) is sent as
# X-Relay-Key so GO can authenticate it — GO holds that same value as Relay.ingress_api_key.
GO_STREAM_ENDED_URL = os.getenv("GO_STREAM_ENDED_URL", "")
# The relay's own secret for outbound calls to GO (currently only the stream-ended
# notification). Distinct from INTERNAL_API_KEY by design: each side has its own credential,
# so compromising one side does not reveal the key the other side uses. GO stores the same
# value as Relay.ingress_api_key.
GO_API_KEY = os.getenv("GO_API_KEY", "")

# Shared outbound HTTP session for GO notifications. Created once at first use and reused for
# every stream-ended POST, so the TLS/TCP setup cost is paid once per process instead of once
# per stream end (the old code kept a shared http_client at startup for the same reason — see
# start_cleanup_task / stop_http_client). Guarded by the asyncio event loop, which runs single-
# threaded, so lazy creation here is race-free.
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
app = FastAPI(title="cc-live-relay", version="0.5.0")


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

        self._lock = asyncio.Lock()
        self._observer_send_locks: dict[WebSocket, asyncio.Lock] = {}
        self._observer_catchup_limit: dict[WebSocket, int] = {}

    # ── Data ingestion (called from source loop) ─────────────────────────

    async def apply_header(self, ws: WebSocket, payload: bytes) -> None:
        """Store canonical header (first received wins). Broadcast once."""
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

    async def apply_patch(self, ws: WebSocket, payload: bytes) -> None:
        """Apply patch to header at given offset, broadcast to observers."""
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

    async def apply_body(self, ws: WebSocket, payload: bytes) -> None:
        """Append body data. Payload always has [8B offset uint64 LE][data]."""
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
                self.last_active = time.time()
                should_broadcast = True
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
            else:
                log_warn(f"[LIVESTREAMER] [ERROR] BODY gap for game {self.lobby_id}: "
                      f"offset={offset} > body_len={body_len} — dropping, investigate source")

        if should_broadcast:
            file_offset = len(self.header) + offset
            framed = struct.pack('<Q', file_offset) + data
            await self._broadcast_envelope(MSG_BODY, framed, targets=targets)

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
        # The relay owns closing a stream: when it observes the last source leave (with or
        # without END), it reports the end to GO so GO stops listing the livestream. This is
        # the primary teardown signal — GO's DELETE /internal/livestreams is only a fallback
        # for cases the relay cannot observe (e.g. GO knows the match ended before the last
        # source disconnected).
        if ended_here:
            await notify_stream_ended(self.lobby_id, "all sources gone")

    # ── Observer lifecycle ───────────────────────────────────────────────

    async def add_observer(self, ws: WebSocket) -> Optional[asyncio.Lock]:
        """Register an observer, returning its send lock *already held*.

        The lock is taken before the socket joins observer_ws_set — that is, before it
        becomes a target for _broadcast_envelope. Otherwise a live BODY chunk could be
        delivered ahead of the catch-up chunks that precede it, and since observers write
        each chunk at its absolute file offset, that leaves a hole in the observer's file.
        The old client tolerated it by accident (its playhead ran far behind the tail);
        the parse cursor added for the broadcast delay would stall on it instead.

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
            # Body length at the instant this observer became a broadcast target. Catch-up
            # sends up to exactly here and live broadcasts carry on from it, so every byte
            # is delivered exactly once — no hole, and no overlapping resend either.
            self._observer_catchup_limit[ws] = len(self.body)
            self.observer_ws_set.add(ws)
            self.last_active = time.time()
            return send_lock

    async def remove_observer(self, ws: WebSocket) -> None:
        async with self._lock:
            self.observer_ws_set.discard(ws)
            self._observer_send_locks.pop(ws, None)
            self._observer_catchup_limit.pop(ws, None)

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
            delay_snapshot = self.delay_seconds
            # Stop exactly where live broadcasts to this observer begin. Snapshotting the
            # whole body instead would resend anything appended between registration and
            # now — data the observer is also about to receive as a live chunk.
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

        if ended_snapshot:
            await ws.send_bytes(pack_frame(MSG_END, b''))

        log_debug(f"[OBSERVER] [CATCHUP] Sent header ({len(header_snapshot)}B) + body ({len(body_snapshot)}B, offset={last_offset}) to observer")

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

        async def send_one(ws: WebSocket) -> None:
            lock = self._observer_send_locks.get(ws)
            if lock is None:
                return    # already removed, or never fully registered
            try:
                async with lock:
                    await ws.send_bytes(frame)
            except Exception as e:
                log_warn(f"[OBSERVER] [WARN] send to observer failed ({type(e).__name__}: {e}), marking dead")
                dead.append(ws)

        # Concurrent, not sequential: a single slow/laggy observer must not delay delivery to
        # every other observer of this game. Previously this was a plain `for` loop awaiting
        # each send in turn, which measured as multi-second tail latency once a game had more
        # than ~50-150 concurrent observers (see plans/relay-scaling-rework.md, "Load test
        # findings"). `dead.append` from concurrent tasks is safe without a lock: asyncio tasks
        # are cooperatively scheduled on one thread, so list.append can't interleave.
        await asyncio.gather(*(send_one(ws) for ws in
                                (targets if targets is not None else list(self.observer_ws_set))))

        for ws in dead:
            async with self._lock:
                self.observer_ws_set.discard(ws)
                self._observer_send_locks.pop(ws, None)
                self._observer_catchup_limit.pop(ws, None)


# ── In-memory state ────────────────────────────────────────────────────────
games: dict[str, GameSession] = {}

# Single-use credentials minted on GO's behalf via /internal/* (plans/
# relay-go-orchestrated-livestreams.md). In-process for now — becomes Redis once the
# dispatcher tier in plans/relay-scaling-rework.md exists and needs the same lookup shared
# across processes. Key -> {lobby_id, user_id, expires_at} (expires_at unix seconds).
watch_tickets: dict[str, dict] = {}
stream_tokens: dict[str, dict] = {}


def _new_credential(lobby_id: str, user_id, store: dict) -> str:
    key = secrets.token_urlsafe(24)
    store[key] = {
        "lobby_id": lobby_id,
        "user_id": user_id,
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


async def notify_stream_ended(lobby_id: str, reason: str) -> None:
    """Tell GO services a livestream has ended, so it can stop listing it.

    Fire-and-forget and never raises: GO being down or unreachable must not affect the
    relay's own teardown. Authenticates with the relay's own key (GO_API_KEY) as
    X-Relay-Key, which GO validates against Relay.ingress_api_key.
    """
    if not GO_STREAM_ENDED_URL:
        return
    try:
        async with _get_go_notify_session().post(
            GO_STREAM_ENDED_URL,
            json={"lobby_id": str(lobby_id), "reason": reason},
            headers={"X-Relay-Key": GO_API_KEY} if GO_API_KEY else {},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as _:
            pass
        log_debug(f"[LIVESTREAM] notified GO: {lobby_id} ended ({reason})")
    except Exception as e:
        log_warn(f"[LIVESTREAM] [WARN] failed to notify GO for {lobby_id}: {type(e).__name__}: {e}")


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


def _public_base(request: Request, lobby_id: str) -> str:
    return f"{PUBLIC_WS_SCHEME}://{_public_host(request)}/stream/{lobby_id}"


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
    """404 unless a live (not ended) session exists for this lobby."""
    session = games.get(lobby_id)
    if not session or session.ended:
        raise HTTPException(status_code=404, detail="game not found or ended")


def _mint_credential(request: Request, lobby_id: str, body: dict,
                     store: dict, url_path: str, query_param: str) -> dict:
    """Mint a single-use credential and return its public connect URL."""
    user_id = body.get("user_id")
    key = _new_credential(lobby_id, user_id, store)
    log_debug(f"[TICKET] [INTERNAL] {query_param} minted for user_id={user_id} lobby={lobby_id}")
    return {"url": f"{PUBLIC_WS_SCHEME}://{_public_host(request)}/{url_path}/{lobby_id}?{query_param}={key}"}


@app.post("/internal/livestreams")
async def internal_create_livestream(request: Request):
    """GO announces a new livestream for an in-game lobby. Creates the relay session."""
    body = await _internal_payload(request)
    lobby_id = _require_lobby_id(body)

    session = games.get(lobby_id)
    if session is None or session.ended:
        session = GameSession(lobby_id)
        games[lobby_id] = session
    session.owner_user_id = body.get("owner_user_id")
    log_debug(f"[TICKET] [INTERNAL] livestream registered for lobby={lobby_id}")
    return {"base_url": _public_base(request, lobby_id)}


@app.post("/internal/stream_tokens")
async def internal_create_stream_token(request: Request):
    """Mint a single-use stream token for one lobby member (a streamer)."""
    body = await _internal_payload(request)
    lobby_id = _require_lobby_id(body)
    _require_live_session(lobby_id)
    return _mint_credential(request, lobby_id, body, stream_tokens, "stream", "stream_token")


@app.post("/internal/watch_tickets")
async def internal_create_watch_ticket(request: Request):
    """Mint a single-use watch ticket for one observer of a livestream."""
    body = await _internal_payload(request)
    lobby_id = _require_lobby_id(body)
    _require_live_session(lobby_id)
    return _mint_credential(request, lobby_id, body, watch_tickets, "watch", "ticket")


@app.delete("/internal/livestreams/{lobby_id}")
async def internal_delete_livestream(lobby_id: str, request: Request):
    """GO reports the match ended. Ends the session and reaps it."""
    _require_internal_key(request)
    session = games.get(lobby_id)
    if not session:
        raise HTTPException(status_code=404, detail="game not found")
    session.ended = True
    await session._broadcast_envelope(MSG_END, b'')
    games.pop(lobby_id, None)
    log_debug(f"[TICKET] [INTERNAL] livestream ended lobby={lobby_id}")
    return {"detail": "ok"}


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
    if consume_stream_token(token, lobby_id) is None:
        await websocket.send_bytes(pack_frame(MSG_ERROR, b"Invalid or expired stream token"))
        await websocket.close()
        return

    try:
        # ── Receive REGISTER frame (binary) ────────────────────────────
        msg = await websocket.receive()
        if "bytes" not in msg:
            await websocket.send_bytes(pack_frame(MSG_ERROR, b"Expected binary REGISTER frame"))
            await websocket.close()
            return

        raw_bytes = msg["bytes"]
        log_debug(f"[LIVESTREAMER] [REGISTER_RAW] {len(raw_bytes)} bytes: {raw_bytes[:80].hex()} ...")
        msg_type, payload = unpack_frame(raw_bytes)
        if msg_type != MSG_REGISTER or not payload:
            await websocket.send_bytes(pack_frame(MSG_ERROR, b"Expected REGISTER message (type=0)"))
            await websocket.close()
            return

        reg_text = payload.decode("utf-8", errors="replace")
        log_debug(f"[LIVESTREAMER] [REGISTER] received: {repr(reg_text[:200])}")
        reg = json.loads(reg_text)

        player_name = reg.get("player_name", "unknown")
        can_stream = reg.get("can_stream", False)
        is_host = bool(reg.get("is_host", False))

        if not can_stream:
            await websocket.send_bytes(pack_frame(MSG_ERROR, b"stream token valid, but can_stream required"))
            await websocket.close()
            return

        # ── Assign session ─────────────────────────────────────────────
        # The session is keyed by the URL lobby_id, which is GO's LobbyID (the value GO
        # minted the stream token for). A REGISTER payload carrying a different lobbyid is
        # ignored — the URL is the authority here.
        session = games.get(lobby_id)
        if session is None or session.ended:
            session = GameSession(lobby_id)
            games[lobby_id] = session

        # ── Host-authoritative fields ──────────────────────────────────
        if is_host:
            lobby = sanitize_lobby(reg.get("lobby"))
            if lobby:
                session.lobby = lobby
                log_debug(f"[LIVESTREAMER] [REGISTER] host described lobby "
                          f"'{lobby.get('name', '')}' on '{lobby.get('mapname', '')}' "
                          f"({len(lobby_player_names(lobby))} players) for {session.lobby_id}")

            raw_delay = reg.get("delay_seconds")
            if raw_delay is not None:
                try:
                    session.delay_seconds = max(0, min(int(raw_delay), MAX_DELAY_SECONDS))
                    log_debug(f"[LIVESTREAMER] [DELAY] Game {session.lobby_id}: "
                          f"delay_seconds={session.delay_seconds}")
                except (TypeError, ValueError):
                    log_warn(f"[LIVESTREAMER] [WARN] bad delay_seconds={raw_delay!r}, "
                          f"keeping {session.delay_seconds}")

        role = "streamer"
        async with session._lock:
            session.sources.add(websocket)

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
        elif msg_type == MSG_END:
            async with session._lock:
                session.end_received = True
            log_debug(f"[LIVESTREAMER] [END] Source sent END for game {session.lobby_id}")
            break


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
    if consume_watch_ticket(ticket, lobby_id) is None:
        await websocket.send_bytes(pack_frame(MSG_ERROR, b"Missing or invalid watch ticket"))
        await websocket.close()
        return

    session = games.get(lobby_id)
    if not session or session.ended:
        await websocket.send_bytes(pack_frame(MSG_ERROR, b"Game not found or ended"))
        await websocket.close()
        return

    send_lock = await session.add_observer(websocket)
    if send_lock is None:
        await websocket.send_bytes(pack_frame(MSG_ERROR, b"Max observers reached"))
        await websocket.close()
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

    except WebSocketDisconnect:
        log_debug(f"[OBSERVER] [WATCH] Observer disconnected from game {lobby_id}")
    except Exception as e:
        log_debug(f"[OBSERVER] [WATCH] Observer error: {e}")
    finally:
        await session.remove_observer(websocket)


# ═══════════════════════════════════════════════════════════════════════════
# Background cleanup
# ═══════════════════════════════════════════════════════════════════════════

@app.on_event("startup")
async def start_cleanup_task():
    asyncio.create_task(_cleanup_loop())


@app.on_event("shutdown")
async def stop_http_client():
    """Close the shared outbound session so the process exits cleanly."""
    global _go_notify_session
    if _go_notify_session is not None:
        await _go_notify_session.close()
        _go_notify_session = None


async def _cleanup_loop():
    """Periodically remove ended/inactive/undescribed games; purge expired credentials."""
    while True:
        await asyncio.sleep(15)
        now = time.time()

        for store in (watch_tickets, stream_tokens):
            expired = [k for k, c in store.items() if c["expires_at"] < now]
            for k in expired:
                store.pop(k, None)

        to_remove = []
        notify_reasons = {}
        for lobby_id, session in games.items():
            if session.ended or (now - session.last_active > INACTIVE_GAME_TTL):
                to_remove.append(lobby_id)
                # A session already marked `ended` was closed by remove_source, which already
                # notified GO. A session reaped purely for inactivity was never closed by a
                # source disconnect, so the relay must report it here.
                if not session.ended:
                    notify_reasons[lobby_id] = "inactivity"
                continue

            # A session nobody ever claimed as host can never be listed or watched, but an
            # active non-host source keeps last_active fresh forever, so the inactivity TTL
            # above never reaches it. Bound the wasted upload rather than letting it run for
            # the whole match.
            if not session.lobby and (now - session.created_at > UNDESCRIBED_GAME_TTL):
                log_warn(f"[LIVESTREAMER] [CLEANUP] No host registration for {lobby_id} after "
                         f"{UNDESCRIBED_GAME_TTL}s; dropping (host not streaming?)")
                to_remove.append(lobby_id)
                notify_reasons[lobby_id] = "undescribed"

        for lobby_id in to_remove:
            session = games.pop(lobby_id, None)
            if session:
                try:
                    await session._broadcast_envelope(MSG_END, b'')
                except Exception:
                    pass
                log_debug(f"[LIVESTREAMER] [CLEANUP] Removed game {lobby_id}")
                reason = notify_reasons.get(lobby_id)
                if reason:
                    await notify_stream_ended(lobby_id, reason)


# ═══════════════════════════════════════════════════════════════════════════
# Startup / main
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    host = os.getenv("HOST", "0.0.0.0")
    print(f"[START] cc-live-relay v0.5.0 starting on {host}:{PORT}")
    print(f"[START] Max observers: {MAX_OBSERVERS_PER_GAME}, Chunk size: {CHUNK_SIZE} bytes")
    if not INTERNAL_API_KEY:
        print("[START] WARNING: INTERNAL_API_KEY is not set — /internal/* endpoints will refuse all calls")
    if GO_STREAM_ENDED_URL and not GO_API_KEY:
        print("[START] WARNING: GO_STREAM_ENDED_URL is set but GO_API_KEY is not — "
              "GO will reject the stream-ended notification (401)")
    uvicorn.run(app, host=host, port=PORT)
