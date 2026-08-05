"""
cc-live-relay — Live game relay server for Generals Zero Hour

Architecture: Source → Relay → Observer
WebSocket-based relay with binary envelope protocol (msg types 0-6).
Aligned with the C++ LiveStreamer/LiveObserver client (libcurl websockets).
"""

import asyncio
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
MAX_OBSERVERS_PER_GAME = int(os.getenv("MAX_OBSERVERS_PER_GAME", "200"))
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

# ── Auth / access-control config (plans/go-auth-and-safeguards.md) ────────────
# Both gates default OFF: this build ships the mechanism so it can be exercised directly
# (curl the ticket endpoint, flip the env var in a test deployment) without yet requiring it,
# because the GameClient side (a new ticket-fetch step before connecting) hasn't landed. Turning
# REQUIRE_WATCH_AUTH on before that ships would lock out every current observer.
REQUIRE_WATCH_AUTH = os.getenv("REQUIRE_WATCH_AUTH", "").strip().lower() in ("1", "true", "yes", "on")
ENABLE_SELF_VIEW_BLOCK = os.getenv("ENABLE_SELF_VIEW_BLOCK", "").strip().lower() in ("1", "true", "yes", "on")

GO_USERS_ME_URL = os.getenv(
    "GO_USERS_ME_URL", "https://api.playgenerals.online/env/prod/contract/1/Users/Me")
# Long enough to cover a real client's connect time (observed up to ~4s under a heavy
# simultaneous-connect burst during load testing), short enough to keep the replay window for a
# stolen ticket small.
WATCH_TICKET_TTL_SECONDS = int(os.getenv("WATCH_TICKET_TTL_SECONDS", "30"))
# What scheme/host the ticket's returned URL uses. Not derived from the request's own URL/Host
# header, since those aren't trustworthy indicators of the public scheme until a reverse proxy
# that sets X-Forwarded-Proto sits in front (see plans/relay-scaling-rework.md) — explicit
# config avoids guessing wrong and handing out a URL the client can't actually connect to.
PUBLIC_WS_SCHEME = os.getenv("PUBLIC_WS_SCHEME", "wss")


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
app = FastAPI(title="cc-live-relay", version="0.4.0")


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
# REGISTER, using GO's own key spelling, and the relay republishes it in /games. A client
# therefore parses the same structure whether the game list came from here or, one day, from GO
# itself — no translation layer on either side.
#
# The allow-lists are the point of this, not a formality: a GO lobby also carries a password,
# per-member ports and an anticheat id, none of which are a third-party viewer's business. Only
# these keys survive into a session, so the relay can never become an accidental republisher of
# something a client should not have sent in the first place.
LOBBY_KEYS = ("lobbytype", "region", "rngseed", "mapname", "mappath", "name", "owner")
LOBBY_MEMBER_KEYS = ("userid", "displayname")

# Defaults, so every /games row has the full key set even when the streamer sent no lobby block
# (an older client, or one that started a match without a lobby cache). Empty rather than absent
# keeps the client's parsing free of per-key existence checks.
LOBBY_DEFAULTS = {
    "lobbytype": -1,
    "region": "",
    "rngseed": -1,
    "mapname": "",
    "mappath": "",
    "name": "",
    "owner": -1,
    "members": [],
}


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
        # Self-view prevention (plans/go-auth-and-safeguards.md Phase 2): every IP that has
        # registered as a source for this session. IP-based, not account-based — the streamer's
        # own machine is the only IP the relay can attribute with confidence; a shared
        # household/NAT false-positive is an accepted tradeoff, see that plan.
        self.source_ips: set[str] = set()

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
        async with self._lock:
            self.sources.discard(ws)
            if not self.sources and self.end_received:
                self.ended = True
                should_broadcast_end = True
                should_save = True
                log_debug(f"[LIVESTREAMER] [END] Game {self.lobby_id}: all sources gone, END was received")
            elif not self.sources:
                self.ended = True
                should_save = True
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

# Watch tickets (plans/go-auth-and-safeguards.md Phase 1b): key -> {user_id, lobby_id,
# expires_at}. In-process for now — becomes a Redis entry once the dispatcher tier in
# plans/relay-scaling-rework.md exists and needs the same lookup shared across processes.
watch_tickets: dict[str, dict] = {}

# Created once at startup, reused for every Users/Me call — avoids paying TLS/TCP setup cost
# per ticket mint. See start_cleanup_task/shutdown below.
http_client: Optional[aiohttp.ClientSession] = None


def mint_watch_ticket(lobby_id: str, user_id) -> dict:
    """Create a single-use ticket admitting one /watch connection to this lobby."""
    key = secrets.token_urlsafe(24)
    watch_tickets[key] = {
        "user_id": user_id,
        "lobby_id": lobby_id,
        "expires_at": time.time() + WATCH_TICKET_TTL_SECONDS,
    }
    return {"key": key, "expires_in": WATCH_TICKET_TTL_SECONDS}


def consume_watch_ticket(key: Optional[str], lobby_id: str) -> Optional[dict]:
    """Validate and burn a ticket. Returns the ticket dict on success, else None.

    Pops unconditionally (even on a lobby_id mismatch) so a ticket is single-use regardless of
    which check fails it — a client can't retry a stolen/mismatched ticket against a different
    lobby_id after a first failed attempt.
    """
    if not key:
        return None
    ticket = watch_tickets.pop(key, None)
    if ticket is None:
        return None
    if ticket["lobby_id"] != lobby_id or ticket["expires_at"] < time.time():
        return None
    return ticket


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


@app.get("/debug/body/{lobby_id}")
async def debug_body(
    lobby_id: str,
    offset: int = 0,
    limit: int = 200,
):
    """Inspect raw body bytes for a game (hex preview, for debugging)."""
    session = games.get(lobby_id)
    if not session:
        return {"error": "game not found"}
    body = bytes(session.body)
    result_slice = body[offset:]
    if limit > 0:
        result_slice = result_slice[:limit]
    return {
        "lobbyid": session.lobby_id,
        "body_bytes": len(body),
        "header_bytes": len(session.header),
        "offset": offset,
        "returned": len(result_slice),
        "data_hex": result_slice.hex()[:1000],
        "data_preview": repr(result_slice[:200]),
    }


@app.get("/watch/{lobby_id}/ticket")
async def mint_ticket(lobby_id: str, request: Request):
    """Mint a short-lived, single-use ticket admitting one /watch/{lobby_id} connection.

    plans/go-auth-and-safeguards.md Phase 1b. Always performs full validation regardless of
    REQUIRE_WATCH_AUTH — that flag only controls whether /watch *requires* a valid ticket, so
    this endpoint is safe to exercise (and its failure paths safe to test) before the
    GameClient-side ticket-fetch step exists. GO cannot be validated locally: session tokens are
    signed with a symmetric key the relay does not and should not hold (see that plan's
    "Confirmed facts (GO services backend)"), so this always costs one real network round-trip.
    """
    session = games.get(lobby_id)
    if not session or session.ended:
        raise HTTPException(status_code=404, detail="game not found or ended")

    # Forwarded via header, never the URL/query string, and only over HTTPS (GO_USERS_ME_URL
    # defaults to https:// — a misconfigured http:// override is caught below rather than
    # silently sending the token in the clear). Never logged: no log line in this function (or
    # anywhere else in the file) includes auth_header or any part of it.
    if not GO_USERS_ME_URL.startswith("https://"):
        log_warn(f"[TICKET] GO_USERS_ME_URL is not https:// ({GO_USERS_ME_URL!r}) — "
                  f"the session token would be sent unencrypted; refusing")
        raise HTTPException(status_code=500, detail="auth endpoint misconfigured")

    auth_header = request.headers.get("authorization")
    if not auth_header:
        raise HTTPException(status_code=401, detail="missing Authorization header")

    assert http_client is not None
    try:
        async with http_client.get(
            GO_USERS_ME_URL,
            headers={"Authorization": auth_header},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as resp:
            # Forwarded faithfully rather than collapsed to a single pass/fail, so a future
            # GameClient build can show the user something more specific than "try again":
            #   200 -> valid JWT, user found                    -> proceed
            #   401 -> invalid/expired JWT (GO's JWT middleware rejects before the controller
            #          body runs, so this is the *only* code the live endpoint currently
            #          returns for a bad token)
            #   403 -> valid JWT, but the account is banned/unauthorized
            #   404 -> valid JWT, but the account no longer exists (deleted user)
            # 403/404 are not emitted by the Users/Me controller as it exists today (checked
            # directly against the public source, GenOnlineService/Controllers/User/UserController.cs
            # — MyUser() has no branch for either case, only the JWT-bearer middleware's 401)
            # — handled here defensively so the relay needs no further change if/when GO adds
            # that distinction upstream. Until then only 200/401 will actually occur.
            if resp.status == 200:
                user_data = await resp.json()
            elif resp.status == 401:
                raise HTTPException(status_code=401, detail="invalid or expired session token")
            elif resp.status == 403:
                raise HTTPException(status_code=403, detail="account banned or unauthorized")
            elif resp.status == 404:
                raise HTTPException(status_code=404, detail="account no longer exists")
            else:
                log_warn(f"[TICKET] unexpected Users/Me status {resp.status}")
                raise HTTPException(status_code=502, detail="unexpected auth service response")
    except aiohttp.ClientError as e:
        log_warn(f"[TICKET] Users/Me call failed: {type(e).__name__}: {e}")
        raise HTTPException(status_code=503, detail="auth service unavailable")

    # user_id is the identity signal, not display_name — an empty display_name can come from a
    # DB error on GO's side while Users/Me still returns 200, so it must never be treated as
    # "invalid user" here. This code never branches on display_name at all, by design.
    #
    # The 200 body itself is still validated: the real response (confirmed against the public
    # controller source) is {"user_id": <Int64>, "display_name": <str>}, but a body that isn't
    # an object with an integer user_id — a camelCased key rename, a list, a proxy serving a
    # 200 error page, user_id arriving as string/bool — means the contract changed, and minting
    # a ticket with user_id=None would silently poison every downstream consumer of the
    # identity. Fail loudly instead; the ticket-mint call costs a real network round-trip
    # anyway, so a degraded upstream is worth surfacing as a 502 rather than papering over.
    if not isinstance(user_data, dict):
        log_warn(f"[TICKET] Users/Me 200 body was not a JSON object: {type(user_data).__name__}")
        raise HTTPException(status_code=502, detail="auth service returned an unexpected response")
    user_id = user_data.get("user_id")
    if not isinstance(user_id, int) or isinstance(user_id, bool):
        log_warn(f"[TICKET] Users/Me 200 body missing integer user_id: {user_data!r}")
        raise HTTPException(status_code=502, detail="auth service returned an unexpected response")

    if ENABLE_SELF_VIEW_BLOCK:
        requester_ip = request.client.host if request.client else None
        if requester_ip and requester_ip in session.source_ips:
            raise HTTPException(status_code=403, detail="cannot watch your own stream")

    ticket = mint_watch_ticket(lobby_id, user_id)
    host = request.headers.get("host") or request.url.hostname
    url = f"{PUBLIC_WS_SCHEME}://{host}/watch/{lobby_id}?ticket={ticket['key']}"
    log_debug(f"[TICKET] minted for user_id={user_id} lobby={lobby_id}, expires_in={ticket['expires_in']}s")
    return {"url": url, "expires_in": ticket["expires_in"]}


@app.get("/games")
async def list_games():
    """Live games available to watch. Backs the in-game observer browser.

    Each row is GeneralsOnline's own lobby shape (LOBBY_KEYS + members, flat, GO's key
    spelling) with the relay's per-session fields alongside it. Keeping the descriptive
    half identical to GO means the client's row-parsing code does not care which service
    produced the list.
    """
    now = time.time()
    result = []
    for g in games.values():
        if g.ended:
            continue

        # No host registration yet means nothing authoritative is known about this game — only
        # that someone is pushing bytes for it. Listing it would show a row with a blank name
        # and no players, so it stays hidden until the host describes it (see UNDESCRIBED_TTL,
        # which reaps it if the host never does).
        if not g.lobby:
            continue

        entry = dict(LOBBY_DEFAULTS)
        entry.update(g.lobby)
        entry.update({
            "lobbyid": g.lobby_id,
            # GO reports the lobby's own creation time; the relay only ever sees a game at
            # REGISTER, so this is when THIS SESSION started, which is also what
            # age_seconds counts from. Same field name and ISO-8601 UTC format as GO so a
            # shared parser works, but do not read it as the lobby's creation time.
            "timecreated": datetime.fromtimestamp(g.created_at, timezone.utc)
                                   .isoformat().replace("+00:00", "Z"),
            "viewers": len(g.observer_ws_set),
            "body_bytes": len(g.body),
            "sources": len(g.sources),
            # The observer applies this delay, so showing it up front sets the
            # expectation of how far behind live the view will be.
            "delay_seconds": g.delay_seconds,
            # How long this game has been streaming. Joining a long-running game
            # means starting well behind live, which is worth seeing before you commit.
            "age_seconds": int(now - g.created_at),
        })
        result.append(entry)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# WebSocket /register (sources)
# ═══════════════════════════════════════════════════════════════════════════

@app.websocket("/register")
async def register_endpoint(websocket: WebSocket):
    """
    A source registers here. Any client with can_stream=True becomes a source.
    No more streamer/backup distinction — everyone sends continuously.

    Protocol (binary):
    1. Client sends REGISTER frame (type=0), payload = JSON with lobbyid/can_stream/player_name
    2. Server sends ROLE frame (type=5), payload = JSON {"role":"streamer","lobbyid":"..."}
    3. Source sends HEADER (type=1), then PATCH/BODY/END (type=2/3/4)
    """
    await websocket.accept()
    session: Optional[GameSession] = None
    role: str = "unknown"
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
        log_debug(f"[LIVESTREAMER] [REGISTER_DECODE] type={msg_type} payload_len={len(payload) if payload else 0}")
        if msg_type != MSG_REGISTER or not payload:
            await websocket.send_bytes(pack_frame(MSG_ERROR, b"Expected REGISTER message (type=0)"))
            await websocket.close()
            return

        reg_text = payload.decode("utf-8", errors="replace")
        log_debug(f"[LIVESTREAMER] [REGISTER] received: {repr(reg_text[:200])}")
        # The client escapes its own JSON now (liveStreamJsonEscape), so a payload that does
        # not parse is genuinely broken. This used to retry with every backslash doubled, to
        # cope with raw Windows paths — a hack that a quote in a lobby name would have
        # defeated anyway, and that could only corrupt a payload that was already valid.
        reg = json.loads(reg_text)

        lobby_id = reg.get("lobbyid", "")
        player_name = reg.get("player_name", "unknown")
        can_stream = reg.get("can_stream", False)
        is_host = bool(reg.get("is_host", False))

        if not lobby_id:
            await websocket.send_bytes(pack_frame(MSG_ERROR, b"lobbyid required"))
            await websocket.close()
            return

        # ── Assign session ─────────────────────────────────────────────
        # Any client may open the session, host or not. It is tempting to let only the host
        # create one and reject the rest, but every player in a lobby starts within
        # milliseconds of every other, so a non-host routinely arrives first — rejecting it
        # would drop a perfectly good source over pure arrival order. Instead the session
        # exists for whoever gets there first and stays *undescribed*, and therefore
        # unlisted, until the host fills it in below.
        if lobby_id in games:
            session = games[lobby_id]
            if session.ended:
                session = GameSession(lobby_id)
                games[lobby_id] = session
        else:
            session = GameSession(lobby_id)
            games[lobby_id] = session

        # ── Host-authoritative fields ──────────────────────────────────
        # Only the lobby host describes the game or sets its options. Accepting either from
        # any client made the published description a race between eight simultaneous
        # registrations. A host re-registering (reconnect, host migration) overwrites, since
        # by then it is the authority on what changed.
        if is_host:
            lobby = sanitize_lobby(reg.get("lobby"))
            if lobby:
                session.lobby = lobby
                log_debug(f"[LIVESTREAMER] [REGISTER] host described lobby "
                          f"'{lobby.get('name', '')}' on '{lobby.get('mapname', '')}' "
                          f"({len(lobby_player_names(lobby))} players) for {session.lobby_id}")

            # The host owns the broadcast delay — it is their spoiler window. Taken here, at
            # REGISTER, so it is settled before any observer can connect.
            raw_delay = reg.get("delay_seconds")
            if raw_delay is not None:
                try:
                    session.delay_seconds = max(0, min(int(raw_delay), MAX_DELAY_SECONDS))
                    log_debug(f"[LIVESTREAMER] [DELAY] Game {session.lobby_id}: "
                          f"delay_seconds={session.delay_seconds}")
                except (TypeError, ValueError):
                    log_warn(f"[LIVESTREAMER] [WARN] bad delay_seconds={raw_delay!r}, "
                          f"keeping {session.delay_seconds}")
        elif reg.get("lobby") is not None or reg.get("delay_seconds") is not None:
            log_warn(f"[LIVESTREAMER] [WARN] non-host {player_name} sent host-only fields for "
                     f"{session.lobby_id}; ignored")

        if can_stream:
            role = "streamer"
            client_ip = websocket.client.host if websocket.client else None
            async with session._lock:
                session.sources.add(websocket)
                if client_ip:
                    session.source_ips.add(client_ip)
        else:
            role = "observer"

        # ── Send ROLE response (binary) ────────────────────────────────
        role_json = json.dumps({"role": role, "lobbyid": session.lobby_id,
            "body_offset": len(session.body)}, separators=(',', ':'))
        await websocket.send_bytes(pack_frame(MSG_ROLE, role_json.encode()))
        log_debug(f"[LIVESTREAMER] [REGISTER] {player_name} -> role={role} game={session.lobby_id}...")

        # ── Enter loop ─────────────────────────────────────────────────
        if role == "streamer":
            await _source_loop(websocket, session)
        else:
            await _keep_alive(websocket)

    except WebSocketDisconnect:
        log_debug(f"[LIVESTREAMER] [DISCONNECT] Client disconnected (role={role})")
    except Exception as e:
        log_warn(f"[LIVESTREAMER] [ERROR] /register error: {e}")
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


async def _keep_alive(ws: WebSocket) -> None:
    """Keep-alive for observer-only /register connections."""
    while True:
        msg = await ws.receive()
        if msg.get("type") == "websocket.disconnect":
            break


# ═══════════════════════════════════════════════════════════════════════════
# WebSocket /watch/{lobby_id} (observers)
# ═══════════════════════════════════════════════════════════════════════════

async def admit_observer(websocket: WebSocket, session: GameSession, lobby_id: str) -> bool:
    """Ticket + self-view checks shared by /watch and /watch-reconnect.

    Sends the appropriate MSG_ERROR and closes on rejection. Returns True iff the caller should
    proceed to session.add_observer().
    """
    if REQUIRE_WATCH_AUTH:
        ticket_key = websocket.query_params.get("ticket")
        if consume_watch_ticket(ticket_key, lobby_id) is None:
            await websocket.send_bytes(pack_frame(MSG_ERROR, b"Missing or invalid watch ticket"))
            await websocket.close()
            return False

    if ENABLE_SELF_VIEW_BLOCK:
        observer_ip = websocket.client.host if websocket.client else None
        if observer_ip and observer_ip in session.source_ips:
            await websocket.send_bytes(pack_frame(MSG_ERROR, b"Cannot watch your own stream"))
            await websocket.close()
            return False

    return True


@app.websocket("/watch/{lobby_id}")
async def watch_game(websocket: WebSocket, lobby_id: str):
    """
    An observer connects to watch a game.

    Protocol (binary):
    1. Server sends HEADER (type=1) + BODY chunks (type=3) for catch-up
    2. Server streams live PATCH/BODY/END (type=2/3/4)
    """
    await websocket.accept()
    session = games.get(lobby_id)

    if not session or session.ended:
        await websocket.send_bytes(pack_frame(MSG_ERROR, b"Game not found or ended"))
        await websocket.close()
        return

    if not await admit_observer(websocket, session, lobby_id):
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
# WebSocket /watch-reconnect/{lobby_id}
# ═══════════════════════════════════════════════════════════════════════════

@app.websocket("/watch-reconnect/{lobby_id}")
async def watch_reconnect(websocket: WebSocket, lobby_id: str):
    """
    Observer reconnects with a last_offset hint.

    Client sends: {"type": "reconnect", "last_offset": 12345} (JSON text)
    Server sends: HEADER + BODY[last_offset:] + live stream (binary).
    """
    await websocket.accept()
    session = games.get(lobby_id)

    if not session:
        await websocket.send_bytes(pack_frame(MSG_ERROR, b"Game not found"))
        await websocket.close()
        return

    if not await admit_observer(websocket, session, lobby_id):
        return

    try:
        raw = await websocket.receive_text()
        msg = json.loads(raw)

        if msg.get("type") != "reconnect":
            await websocket.send_bytes(pack_frame(MSG_ERROR, b"Expected type=reconnect"))
            await websocket.close()
            return

        last_offset = msg.get("last_offset", 0)

        send_lock = await session.add_observer(websocket)
        if send_lock is None:
            await websocket.send_bytes(pack_frame(MSG_ERROR, b"Max observers reached"))
            await websocket.close()
            return

        try:
            await websocket.send_json({
                "type": "reconnect",
                "last_offset": last_offset,
                "server_body_bytes": len(session.body),
            })

            await session.send_catchup(websocket, last_offset=last_offset, held_lock=send_lock)
        finally:
            send_lock.release()
        log_debug(f"[OBSERVER] [RECONNECT] Sent body from offset {last_offset} (total body: {len(session.body)} bytes)")

        while True:
            msg = await websocket.receive()
            if msg.get("type") == "websocket.disconnect":
                break

    except WebSocketDisconnect:
        log_debug(f"[OBSERVER] [RECONNECT] Observer disconnected from game {lobby_id}")
    except Exception as e:
        log_debug(f"[OBSERVER] [RECONNECT] Observer error: {e}")
    finally:
        await session.remove_observer(websocket)


# ═══════════════════════════════════════════════════════════════════════════
# Background cleanup
# ═══════════════════════════════════════════════════════════════════════════

@app.on_event("startup")
async def start_cleanup_task():
    global http_client
    http_client = aiohttp.ClientSession()
    asyncio.create_task(_cleanup_loop())


@app.on_event("shutdown")
async def stop_http_client():
    if http_client is not None:
        await http_client.close()


async def _cleanup_loop():
    """Periodically remove ended, inactive, or never-described games; purge expired tickets."""
    while True:
        await asyncio.sleep(15)
        now = time.time()

        expired_tickets = [k for k, t in watch_tickets.items() if t["expires_at"] < now]
        for k in expired_tickets:
            watch_tickets.pop(k, None)

        to_remove = []
        for lobby_id, session in games.items():
            if session.ended or (now - session.last_active > INACTIVE_GAME_TTL):
                to_remove.append(lobby_id)
                continue

            # A session nobody ever claimed as host can never be listed or watched, but an
            # active non-host source keeps last_active fresh forever, so the inactivity TTL
            # above never reaches it. That happens whenever the host has streaming switched
            # off and another player has it on — bound the wasted upload rather than letting
            # it run for the whole match.
            if not session.lobby and (now - session.created_at > UNDESCRIBED_GAME_TTL):
                log_warn(f"[LIVESTREAMER] [CLEANUP] No host registration for {lobby_id} after "
                         f"{UNDESCRIBED_GAME_TTL}s; dropping (host not streaming?)")
                to_remove.append(lobby_id)

        for lobby_id in to_remove:
            session = games.pop(lobby_id, None)
            if session:
                try:
                    await session._broadcast_envelope(MSG_END, b'')
                except Exception:
                    pass
                log_debug(f"[LIVESTREAMER] [CLEANUP] Removed game {lobby_id}")


# ═══════════════════════════════════════════════════════════════════════════
# Startup / main
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    host = os.getenv("HOST", "0.0.0.0")
    print(f"[START] cc-live-relay v0.4.0 starting on {host}:{PORT}")
    print(f"[START] Max observers: {MAX_OBSERVERS_PER_GAME}, Chunk size: {CHUNK_SIZE} bytes")
    uvicorn.run(app, host=host, port=PORT)
