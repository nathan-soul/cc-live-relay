"""
cc-live-relay — Live game relay server voor Generals Zero Hour

Architectuur: Source → Relay → Observer
WebSocket-based relay die game data doorstuurt van bronnen naar observers.
Binair envelope protocol: 1-byte type + 4-byte lengte (uint32 LE) + payload.
"""

import asyncio
import json
import os
import struct
import time
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

# ── Binary envelope types ─────────────────────────────────────────────────
HEADER_TYPE = 1
PATCH_TYPE  = 2
BODY_TYPE   = 3
END_TYPE    = 4

CHUNK_SIZE = 256 * 1024  # 256 KB per chunk voor observer catch-up

# ── Configuratie via environment variables ─────────────────────────────────
PORT = int(os.getenv("PORT", "8765"))
MAX_OBSERVERS_PER_GAME = int(os.getenv("MAX_OBSERVERS_PER_GAME", "200"))
INACTIVE_GAME_TTL = 60

# ── App ────────────────────────────────────────────────────────────────────
app = FastAPI(title="cc-live-relay", version="0.3.0")


# ── Binary envelope helpers ────────────────────────────────────────────────

def encode_envelope(msg_type: int, payload: bytes) -> bytes:
    """1-byte type + 4-byte length (uint32 LE) + payload."""
    return bytes([msg_type]) + struct.pack('<I', len(payload)) + payload


def decode_envelope(data: bytes) -> tuple[int, bytes]:
    """Decode binary envelope, returns (type, payload)."""
    if len(data) < 5:
        raise ValueError(f"Envelope too short: {len(data)} bytes")
    msg_type = data[0]
    length = struct.unpack('<I', data[1:5])[0]
    if len(data) < 5 + length:
        raise ValueError(f"Envelope truncated: expected {5+length} bytes, got {len(data)}")
    return msg_type, data[5:5 + length]


# ── GameSession ────────────────────────────────────────────────────────────

class GameSession:
    """One active game: multiple sources, multiple observers."""

    def __init__(self, game_hash: str):
        self.game_id: str = game_hash
        self.game_hash: str = game_hash
        self.map_name: str = ""
        self.players: list = []
        self.created_at: float = time.time()
        self.last_active: float = time.time()

        self.header: bytearray = bytearray()
        self.header_received: bool = False
        self.body: bytearray = bytearray()
        self.ended: bool = False
        self.end_received: bool = False

        self.sources: set[WebSocket] = set()
        self.observer_ws_set: set[WebSocket] = set()

        self._lock = asyncio.Lock()

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
                print(f"[HEADER] Game {self.game_hash[:12]}: stored header ({len(payload)} bytes)")
            elif bytes(self.header) != payload:
                print(f"[WARN] HEADER mismatch from another source for game {self.game_hash[:12]}: "
                      f"stored={len(self.header)}B, received={len(payload)}B")
        if should_broadcast:
            await self._broadcast_envelope(HEADER_TYPE, payload)

    async def apply_patch(self, ws: WebSocket, payload: bytes) -> None:
        """Apply patch to header at given offset, broadcast to observers."""
        if len(payload) < 8:
            print(f"[WARN] PATCH payload too short: {len(payload)} bytes")
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
            print(f"[PATCH] Game {self.game_hash[:12]}: offset={offset} len={patch_len} header_size={len(self.header)}")
        await self._broadcast_envelope(PATCH_TYPE, payload)

    async def apply_body(self, ws: WebSocket, payload: bytes) -> None:
        """Append body data at offset. Deduplicate, detect desync, handle gaps."""
        if len(payload) < 8:
            print(f"[WARN] BODY payload too short: {len(payload)} bytes")
            return
        offset = struct.unpack('<Q', payload[0:8])[0]
        data = payload[8:]

        should_broadcast = False
        async with self._lock:
            body_len = len(self.body)

            if offset == body_len:
                self.body.extend(data)
                self.last_active = time.time()
                should_broadcast = True
                if len(self.body) < 5000 or len(self.body) % 50000 == 0:
                    print(f"[BODY] Game {self.game_hash[:12]}: +{len(data)}B @ offset={offset} total={len(self.body)}")
            elif offset < body_len:
                overlap = min(len(data), body_len - offset)
                existing = bytes(self.body[offset:offset + overlap])
                if data[:overlap] != existing:
                    print(f"[WARN] BODY desync for game {self.game_hash[:12]}: "
                          f"offset={offset} overlap={overlap} mismatch!")
            else:
                print(f"[WARN] BODY gap for game {self.game_hash[:12]}: "
                      f"offset={offset} > body_len={body_len}")
                should_broadcast = True

        if should_broadcast:
            await self._broadcast_envelope(BODY_TYPE, payload)

    # ── Source lifecycle ─────────────────────────────────────────────────

    async def remove_source(self, ws: WebSocket) -> None:
        """Called when a source disconnects. Ends session if all sources gone + END received."""
        should_broadcast_end = False
        async with self._lock:
            self.sources.discard(ws)
            if not self.sources and self.end_received:
                self.ended = True
                should_broadcast_end = True
                print(f"[END] Game {self.game_hash[:12]}: all sources gone, END was received")
            else:
                print(f"[SOURCE_GONE] source disconnected from game {self.game_hash[:12]}... "
                      f"({len(self.sources)} remaining)")
        if should_broadcast_end:
            await self._broadcast_envelope(END_TYPE, b'')

    # ── Observer lifecycle ───────────────────────────────────────────────

    async def add_observer(self, ws: WebSocket) -> bool:
        async with self._lock:
            if len(self.observer_ws_set) >= MAX_OBSERVERS_PER_GAME:
                return False
            self.observer_ws_set.add(ws)
            self.last_active = time.time()
            return True

    async def remove_observer(self, ws: WebSocket) -> None:
        async with self._lock:
            self.observer_ws_set.discard(ws)

    async def send_catchup(self, ws: WebSocket, last_offset: int = 0) -> None:
        """Send header + body[last_offset:] in chunks to a single observer."""
        async with self._lock:
            header_snapshot = bytes(self.header)
            body_snapshot = bytes(self.body)
            ended_snapshot = self.ended

        if header_snapshot:
            await ws.send_bytes(encode_envelope(HEADER_TYPE, header_snapshot))

        last_offset = min(last_offset, len(body_snapshot))
        body_slice = body_snapshot[last_offset:]
        for chunk_off in range(0, len(body_slice), CHUNK_SIZE):
            chunk = body_slice[chunk_off:chunk_off + CHUNK_SIZE]
            chunk_payload = struct.pack('<Q', last_offset + chunk_off) + chunk
            await ws.send_bytes(encode_envelope(BODY_TYPE, chunk_payload))

        if ended_snapshot:
            await ws.send_bytes(encode_envelope(END_TYPE, b''))

    # ── Broadcast ────────────────────────────────────────────────────────

    async def _broadcast_envelope(self, msg_type: int, payload: bytes) -> None:
        """Send binary envelope to every connected observer. Removes dead connections."""
        envelope = encode_envelope(msg_type, payload)
        dead: list[WebSocket] = []
        for ws in list(self.observer_ws_set):
            try:
                await ws.send_bytes(envelope)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.observer_ws_set.discard(ws)


# ── In-memory state ────────────────────────────────────────────────────────
games: dict[str, GameSession] = {}


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


@app.get("/debug/body/{game_id}")
async def debug_body(
    game_id: str,
    offset: int = 0,
    limit: int = 200,
):
    """Inspect raw body bytes for a game (hex preview, for debugging)."""
    session = games.get(game_id)
    if not session:
        return {"error": "game not found"}
    body = bytes(session.body)
    result_slice = body[offset:]
    if limit > 0:
        result_slice = result_slice[:limit]
    return {
        "game_id": session.game_id,
        "body_bytes": len(body),
        "header_bytes": len(session.header),
        "offset": offset,
        "returned": len(result_slice),
        "data_hex": result_slice.hex()[:1000],
        "data_preview": repr(result_slice[:200]),
    }


@app.get("/games")
async def list_games():
    result = []
    for g in games.values():
        if not g.ended:
            result.append({
                "game_id": g.game_id,
                "map": g.map_name,
                "players": g.players,
                "viewers": len(g.observer_ws_set),
                "body_bytes": len(g.body),
                "sources": len(g.sources),
            })
    return result


# ═══════════════════════════════════════════════════════════════════════════
# WebSocket /register (sources)
# ═══════════════════════════════════════════════════════════════════════════

@app.websocket("/register")
async def register_endpoint(websocket: WebSocket):
    """
    Source registreert zich hier. Elke client met can_stream=True wordt source.
    Geen streamer/backup onderscheid meer — iedereen stuurt continu.

    Protocol:
    1. Client stuurt register message (JSON)
    2. Server stuurt role assignment terug (JSON)
    3. Source stuurt HEADER (binary), daarna PATCH/BODY/END (binary)
    """
    await websocket.accept()
    session: Optional[GameSession] = None
    role: str = "unknown"
    try:
        raw = await websocket.receive_text()
        print(f"[REGISTER_RAW] received {len(raw)} bytes: {repr(raw[:300])}")
        reg = json.loads(raw)
        if reg.get("type") != "register":
            await websocket.send_json({"type": "error", "message": "First message must be type=register"})
            await websocket.close()
            return

        game_hash = reg.get("game_hash", "")
        player_name = reg.get("player_name", "unknown")
        can_stream = reg.get("can_stream", False)

        if not game_hash:
            await websocket.send_json({"type": "error", "message": "game_hash required"})
            await websocket.close()
            return

        if game_hash in games:
            session = games[game_hash]
            if session.ended:
                session = GameSession(game_hash)
                games[game_hash] = session
        else:
            session = GameSession(game_hash)
            games[game_hash] = session

        if can_stream:
            role = "streamer"
            async with session._lock:
                session.sources.add(websocket)
        else:
            role = "observer"

        await websocket.send_json({
            "type": "role",
            "role": role,
            "game_id": session.game_id,
        })
        print(f"[REGISTER] {player_name} -> role={role} game={session.game_hash[:12]}...")

        if role == "streamer":
            await _source_loop(websocket, session)
        else:
            await _keep_alive(websocket)

    except WebSocketDisconnect:
        print(f"[DISCONNECT] Client disconnected (role={role})")
    except Exception as e:
        print(f"[ERROR] /register error: {e}")
    finally:
        if session and role == "streamer":
            await session.remove_source(websocket)


async def _source_loop(ws: WebSocket, session: GameSession) -> None:
    """Receive binary envelopes (HEADER/PATCH/BODY/END) from a source."""
    while True:
        msg = await ws.receive()
        if msg.get("type") == "websocket.disconnect":
            break
        if "text" in msg:
            try:
                data = json.loads(msg["text"])
                if data.get("type") == "ping":
                    await ws.send_json({"type": "pong"})
            except json.JSONDecodeError:
                pass
        elif "bytes" in msg:
            raw = msg["bytes"]
            try:
                msg_type, payload = decode_envelope(raw)
            except (struct.error, ValueError) as e:
                print(f"[ERROR] Failed to decode binary envelope: {e}")
                continue

            if msg_type == HEADER_TYPE:
                await session.apply_header(ws, payload)
            elif msg_type == PATCH_TYPE:
                await session.apply_patch(ws, payload)
            elif msg_type == BODY_TYPE:
                await session.apply_body(ws, payload)
            elif msg_type == END_TYPE:
                async with session._lock:
                    session.end_received = True
                print(f"[END] Source sent END for game {session.game_hash[:12]}")
                break


async def _keep_alive(ws: WebSocket) -> None:
    """Keep-alive for observer-only /register connections."""
    while True:
        msg = await ws.receive()
        if msg.get("type") == "websocket.disconnect":
            break


# ═══════════════════════════════════════════════════════════════════════════
# WebSocket /watch/{game_id} (observers)
# ═══════════════════════════════════════════════════════════════════════════

@app.websocket("/watch/{game_id}")
async def watch_game(websocket: WebSocket, game_id: str):
    """
    Observer verbindt om een game te bekijken.

    Protocol:
    1. Server stuurt HEADER (binary) + BODY chunks (binary, catch-up)
    2. Server streamt live PATCH/BODY/END (binary)
    """
    await websocket.accept()
    session = games.get(game_id)

    if not session or session.ended:
        await websocket.send_json({"type": "error", "message": "Game not found or ended"})
        await websocket.close()
        return

    added = await session.add_observer(websocket)
    if not added:
        await websocket.send_json({"type": "error", "message": "Max observers reached"})
        await websocket.close()
        return

    print(f"[WATCH] Observer connected to game {game_id[:12]}... ({len(session.observer_ws_set)} viewers)")

    try:
        await session.send_catchup(websocket, last_offset=0)

        while True:
            msg = await websocket.receive()
            if msg.get("type") == "websocket.disconnect":
                break
            if "text" in msg:
                try:
                    data = json.loads(msg["text"])
                    if data.get("type") == "ping":
                        await websocket.send_json({"type": "pong"})
                except json.JSONDecodeError:
                    pass

    except WebSocketDisconnect:
        print(f"[WATCH] Observer disconnected from game {game_id[:12]}")
    except Exception as e:
        print(f"[WATCH] Observer error: {e}")
    finally:
        await session.remove_observer(websocket)


# ═══════════════════════════════════════════════════════════════════════════
# WebSocket /watch-reconnect/{game_id}
# ═══════════════════════════════════════════════════════════════════════════

@app.websocket("/watch-reconnect/{game_id}")
async def watch_reconnect(websocket: WebSocket, game_id: str):
    """
    Observer reconnect met last_offset hint.

    Client stuurt: {"type": "reconnect", "last_offset": 12345}
    Server stuurt: HEADER + BODY[last_offset:] + live stream.
    """
    await websocket.accept()
    session = games.get(game_id)

    if not session:
        await websocket.send_json({"type": "error", "message": "Game not found"})
        await websocket.close()
        return

    try:
        raw = await websocket.receive_text()
        msg = json.loads(raw)

        if msg.get("type") != "reconnect":
            await websocket.send_json({"type": "error", "message": "Expected type=reconnect"})
            await websocket.close()
            return

        last_offset = msg.get("last_offset", 0)

        added = await session.add_observer(websocket)
        if not added:
            await websocket.send_json({"type": "error", "message": "Max observers reached"})
            await websocket.close()
            return

        await websocket.send_json({
            "type": "reconnect",
            "last_offset": last_offset,
            "server_body_bytes": len(session.body),
        })

        await session.send_catchup(websocket, last_offset=last_offset)
        print(f"[RECONNECT] Sent body from offset {last_offset} (total body: {len(session.body)} bytes)")

        while True:
            msg = await websocket.receive()
            if msg.get("type") == "websocket.disconnect":
                break
            if "text" in msg:
                try:
                    data = json.loads(msg["text"])
                    if data.get("type") == "ping":
                        await websocket.send_json({"type": "pong"})
                except json.JSONDecodeError:
                    pass

    except WebSocketDisconnect:
        print(f"[RECONNECT] Observer disconnected from game {game_id[:12]}")
    except Exception as e:
        print(f"[RECONNECT] Observer error: {e}")
    finally:
        await session.remove_observer(websocket)


# ═══════════════════════════════════════════════════════════════════════════
# Background cleanup
# ═══════════════════════════════════════════════════════════════════════════

@app.on_event("startup")
async def start_cleanup_task():
    asyncio.create_task(_cleanup_loop())


async def _cleanup_loop():
    """Periodically remove ended or inactive games."""
    while True:
        await asyncio.sleep(15)
        now = time.time()
        to_remove = []
        for game_hash, session in games.items():
            if session.ended or (now - session.last_active > INACTIVE_GAME_TTL):
                to_remove.append(game_hash)

        for game_hash in to_remove:
            session = games.pop(game_hash, None)
            if session:
                try:
                    await session._broadcast_envelope(END_TYPE, b'')
                except Exception:
                    pass
                print(f"[CLEANUP] Removed game {game_hash[:12]}...")


# ═══════════════════════════════════════════════════════════════════════════
# Startup / main
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    host = os.getenv("HOST", "0.0.0.0")
    print(f"[START] cc-live-relay v0.3.0 starting on {host}:{PORT}")
    print(f"[START] Max observers: {MAX_OBSERVERS_PER_GAME}, Chunk size: {CHUNK_SIZE} bytes")
    uvicorn.run(app, host=host, port=PORT)
