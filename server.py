"""
cc-live-relay — Live game relay server voor Generals Zero Hour

Architectuur: Streamer → Relay → Observer
WebSocket-based relay die game frames doorstuurt van streamers naar observers.
"""

import asyncio
import json
import os
import time
from collections import deque
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

# ── Configuratie via environment variables ──────────────────────────────────
BUFFER_SECONDS = int(os.getenv("BUFFER_SECONDS", "30"))
PORT = int(os.getenv("PORT", "8765"))
MAX_OBSERVERS_PER_GAME = int(os.getenv("MAX_OBSERVERS_PER_GAME", "200"))
# FPS assumed for ring buffer sizing (default 30)
DEFAULT_FPS = 30
BUFFER_FRAMES = BUFFER_SECONDS * DEFAULT_FPS  # 900 for 30s @ 30fps
INACTIVE_GAME_TTL = 60  # seconds before inactive games are cleaned up

# ── App ────────────────────────────────────────────────────────────────────
app = FastAPI(title="cc-live-relay", version="0.2.0")


# ── GameSession ────────────────────────────────────────────────────────────
class GameSession:
    """One active game: one streamer, zero-or-more backups, zero-or-more observers."""

    def __init__(self, game_hash: str):
        self.game_id: str = game_hash  # game_id == game_hash for simplicity
        self.game_hash: str = game_hash
        self.map_name: str = ""
        self.players: list = []
        self.created_at: float = time.time()
        self.last_active: float = time.time()

        # Frame buffer: stores ALL frames from game start (no size limit).
        # Previously ring-buffered to last 30s, but that dropped early frames
        # that new observers need to reconstruct the full game state.
        self.frame_buffer: deque = deque()

        # Connections
        self.streamer_ws: Optional[WebSocket] = None
        self.backup_ws_list: list[WebSocket] = []
        self.observer_ws_set: set[WebSocket] = set()

        # State
        self.current_frame: int = 0
        self.is_active: bool = True
        self.metadata_sent: bool = False
        self.metadata: Optional[dict] = None

        # Lock for mutations
        self._lock = asyncio.Lock()

    async def add_frame(self, frame_data: dict) -> None:
        """Append a frame to the ring buffer and broadcast to observers."""
        async with self._lock:
            self.current_frame = frame_data.get("frame", self.current_frame + 1)
            self.frame_buffer.append(frame_data)
            self.last_active = time.time()

        # Broadcast outside lock to avoid holding it during sends
        await self._broadcast_to_observers(frame_data)

    async def _broadcast_to_observers(self, msg: dict) -> None:
        """Send a JSON message to every connected observer. Dead connections are removed."""
        dead: list[WebSocket] = []
        for ws in list(self.observer_ws_set):
            try:
                await ws.send_json(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.observer_ws_set.discard(ws)

    async def broadcast_meta(self) -> None:
        """Send metadata to all observers (called once when metadata arrives)."""
        if self.metadata is None:
            return
        async with self._lock:
            self.metadata_sent = True
        msg = {"type": "metadata", **self.metadata}
        await self._broadcast_to_observers(msg)

    async def broadcast_stream_ended(self) -> None:
        """Notify all observers the stream ended."""
        await self._broadcast_to_observers({"type": "stream_ended"})

    async def add_observer(self, ws: WebSocket) -> bool:
        """Add observer if under the limit. Returns True on success."""
        async with self._lock:
            if len(self.observer_ws_set) >= MAX_OBSERVERS_PER_GAME:
                return False
            self.observer_ws_set.add(ws)
            self.last_active = time.time()
            return True

    async def remove_observer(self, ws: WebSocket) -> None:
        async with self._lock:
            self.observer_ws_set.discard(ws)

    async def remove_streamer(self) -> None:
        """Remove the current streamer (on disconnect). Returns True if backup took over."""
        async with self._lock:
            self.streamer_ws = None

        # Try failover
        return await self._try_failover()

    async def _try_failover(self) -> bool:
        """Attempt to promote the first backup to streamer."""
        async with self._lock:
            if self.backup_ws_list:
                new_streamer = self.backup_ws_list.pop(0)
                self.streamer_ws = new_streamer
                self.last_active = time.time()
            else:
                self.is_active = False
                await self.broadcast_stream_ended()
                return False

        # Notify the new streamer (outside lock)
        try:
            await self.streamer_ws.send_json({
                "type": "role",
                "role": "streamer",
                "game_id": self.game_id,
                "action": "takeover",
            })
        except Exception:
            # New streamer is also dead — try again
            return await self._try_failover()
        return True


# ── In-memory state ────────────────────────────────────────────────────────
# game_hash → GameSession
games: dict[str, GameSession] = {}


# ── REST endpoints ─────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    total_observers = sum(len(g.observer_ws_set) for g in games.values())
    return {
        "status": "ok",
        "active_games": sum(1 for g in games.values() if g.is_active),
        "total_observers": total_observers,
    }


@app.get("/games")
async def list_games():
    result = []
    for g in games.values():
        if g.is_active:
            # Show frame range to debug buffer contents
            frame_nums = [f.get("frame", 0) for f in g.frame_buffer] if g.frame_buffer else []
            result.append({
                "game_id": g.game_id,
                "map": g.map_name,
                "players": g.players,
                "viewers": len(g.observer_ws_set),
                "buffer_frames": len(g.frame_buffer),
                "frame_range": f"{min(frame_nums)}-{max(frame_nums)}" if frame_nums else "empty",
                "current_frame": g.current_frame,
            })
    return result


# ── WebSocket /register (streamer / backup / observer-only) ────────────────
@app.websocket("/register")
async def register_endpoint(websocket: WebSocket):
    """
    Streamer of backup registreert zich hier.

    Protocol:
    1. Client stuurt register message
    2. Server stuurt role assignment terug
    3. Streamer stuurt metadata (eenmalig)
    4. Streamer stuurt frames
    """
    await websocket.accept()
    session: Optional[GameSession] = None
    role: str = "unknown"
    try:
        # ── Step 1: wait for register message ────────────────────────────
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

        # ── Step 2: assign role ─────────────────────────────────────────
        if game_hash in games:
            existing = games[game_hash]
            if not existing.is_active:
                # Previous game ended; create fresh session
                session = GameSession(game_hash)
                games[game_hash] = session
                role = "streamer"
                session.streamer_ws = websocket
            elif existing.streamer_ws is None and can_stream:
                # Streamer slot open — take it
                role = "streamer"
                session = existing
                session.streamer_ws = websocket
            elif can_stream:
                # Primary streamer still present — become backup
                role = "backup"
                session = existing
                async with session._lock:
                    session.backup_ws_list.append(websocket)
            else:
                # Observer-only
                role = "observer"
                session = existing
        else:
            # New game — creator becomes streamer
            session = GameSession(game_hash)
            games[game_hash] = session
            role = "streamer"
            session.streamer_ws = websocket

        await websocket.send_json({
            "type": "role",
            "role": role,
            "game_id": session.game_id,
        })
        print(f"[REGISTER] {player_name} → role={role} game={session.game_hash[:12]}...")

        # ── Step 3: streaming loop ──────────────────────────────────────
        if role == "streamer":
            await _streamer_loop(websocket, session)
        elif role == "backup":
            await _backup_loop(websocket, session)
        else:
            # observer-only via /register: keep alive but no frames
            await _keep_alive(websocket)

    except WebSocketDisconnect:
        print(f"[DISCONNECT] Client disconnected (role={role})")
    except Exception as e:
        print(f"[ERROR] /register error: {e}")
    finally:
        if session:
            if role == "streamer":
                await session.remove_streamer()
                print(f"[FAILOVER] Attempted failover for game {session.game_hash[:12]}...")
            elif role == "backup":
                async with session._lock:
                    if websocket in session.backup_ws_list:
                        session.backup_ws_list.remove(websocket)
            elif role == "observer":
                await session.remove_observer(websocket)


async def _streamer_loop(ws: WebSocket, session: GameSession) -> None:
    """Main loop for a primary streamer: receive metadata + frames."""
    metadata_received = False
    while True:
        raw = await ws.receive_text()
        msg = json.loads(raw)
        msg_type = msg.get("type")

        if msg_type == "metadata" and not metadata_received:
            # Store metadata in session
            session.map_name = msg.get("map_name", "")
            session.players = msg.get("players", [])
            session.metadata = {
                "map_name": session.map_name,
                "players": session.players,
                "version": msg.get("version", ""),
                "exe_crc": msg.get("exe_crc", ""),
                "ini_crc": msg.get("ini_crc", ""),
                "game_options": msg.get("game_options", ""),
            }
            metadata_received = True
            await session.broadcast_meta()
            print(f"[METADATA] Game {session.game_hash[:12]}: map={session.map_name} players={session.players}")

        elif msg_type == "frame":
            await session.add_frame(msg)

        elif msg_type == "end":
            print(f"[END] Streamer ended game {session.game_hash[:12]}")
            session.is_active = False
            await session.broadcast_stream_ended()
            break


async def _backup_loop(ws: WebSocket, session: GameSession) -> None:
    """Backup sits idle, waiting for failover. Just keep alive."""
    while True:
        await ws.receive_text()  # discard any messages, just keep connection open


async def _keep_alive(ws: WebSocket) -> None:
    """Generic keep-alive for observer-only /register connections."""
    while True:
        await ws.receive_text()


# ── WebSocket /watch/{game_id} (observers) ────────────────────────────────
@app.websocket("/watch/{game_id}")
async def watch_game(websocket: WebSocket, game_id: str):
    """
    Observer verbindt om een game te bekijken.

    Protocol:
    1. Server stuurt metadata (als beschikbaar)
    2. Server stuurt alle gebufferde frames (catch-up)
    3. Server stuurt real-time frames
    """
    await websocket.accept()
    session = games.get(game_id)

    if not session or not session.is_active:
        await websocket.send_json({"type": "error", "message": "Game not found or ended"})
        await websocket.close()
        return

    # Register observer
    added = await session.add_observer(websocket)
    if not added:
        await websocket.send_json({"type": "error", "message": "Max observers reached"})
        await websocket.close()
        return

    print(f"[WATCH] Observer connected to game {game_id[:12]}... ({len(session.observer_ws_set)} viewers)")

    try:
        # ── Send metadata if available ───────────────────────────────────
        if session.metadata_sent and session.metadata:
            await websocket.send_json({"type": "metadata", **session.metadata})

        # ── Send buffered frames (catch-up) ──────────────────────────────
        if session.frame_buffer:
            # Send ALL catch-up frames in ONE bulk message
            # Must convert deque to list — send_json cannot serialize deque
            await websocket.send_json({
                "type": "catchup_bulk",
                "frames": list(session.frame_buffer),
                "frame_count": len(session.frame_buffer),
                "last_frame": session.current_frame,
            })
        else:
            # No data yet — tell observer to wait
            await websocket.send_json({
                "type": "waiting",
                "message": "Waiting for streamer...",
            })

        # ── Keep alive: listen for observer messages (e.g. reconnect hints) ──
        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)
            msg_type = msg.get("type")

            if msg_type == "request_frame":
                # Observer requests a specific frame from the buffer
                frame_num = msg.get("frame", 0)
                # Walk buffer to find matching frame
                for fb in session.frame_buffer:
                    if fb.get("frame") == frame_num:
                        await websocket.send_json(fb)
                        break

            elif msg_type == "ping":
                await websocket.send_json({"type": "pong"})

    except WebSocketDisconnect:
        print(f"[WATCH] Observer disconnected from game {game_id[:12]}")
    except Exception as e:
        print(f"[WATCH] Observer error: {e}")
    finally:
        await session.remove_observer(websocket)


# ── WebSocket /watch-reconnect/{game_id} (observer reconnect) ─────────────
@app.websocket("/watch-reconnect/{game_id}")
async def watch_reconnect(websocket: WebSocket, game_id: str):
    """
    Observer reconnect with last_frame hint.
    Client sends: {"type": "reconnect", "last_frame": 1234}
    Server responds with frames from last_frame+1 onward.
    """
    await websocket.accept()
    session = games.get(game_id)

    if not session or not session.is_active:
        await websocket.send_json({"type": "error", "message": "Game not found or ended"})
        await websocket.close()
        return

    try:
        raw = await websocket.receive_text()
        msg = json.loads(raw)

        if msg.get("type") != "reconnect":
            await websocket.send_json({"type": "error", "message": "Expected type=reconnect"})
            await websocket.close()
            return

        last_frame = msg.get("last_frame", 0)

        # Register as observer
        added = await session.add_observer(websocket)
        if not added:
            await websocket.send_json({"type": "error", "message": "Max observers reached"})
            await websocket.close()
            return

        # Send metadata if available
        if session.metadata_sent and session.metadata:
            await websocket.send_json({"type": "metadata", **session.metadata})

        # Send reconnect acknowledgement
        await websocket.send_json({
            "type": "reconnect",
            "last_frame": last_frame,
            "server_frame": session.current_frame,
        })

        # Send frames from last_frame+1 as a single bulk message
        filtered_frames = [
            f for f in session.frame_buffer
            if f.get("frame", 0) > last_frame
        ]
        if filtered_frames:
            await websocket.send_json({
                "type": "catchup_bulk",
                "frames": filtered_frames,
                "frame_count": len(filtered_frames),
                "last_frame": session.current_frame,
            })

        print(f"[RECONNECT] Sent {len(filtered_frames)} frames to observer (from frame {last_frame+1})")

        # Now keep alive for real-time frames
        while True:
            await websocket.receive_text()

    except WebSocketDisconnect:
        print(f"[RECONNECT] Observer disconnected from game {game_id[:12]}")
    except Exception as e:
        print(f"[RECONNECT] Observer error: {e}")
    finally:
        await session.remove_observer(websocket)


# ── Background cleanup task ────────────────────────────────────────────────
@app.on_event("startup")
async def start_cleanup_task():
    asyncio.create_task(_cleanup_loop())


async def _cleanup_loop():
    """Periodically remove inactive games."""
    while True:
        await asyncio.sleep(15)
        now = time.time()
        to_remove = []
        for game_hash, session in games.items():
            if not session.is_active or (now - session.last_active > INACTIVE_GAME_TTL):
                to_remove.append(game_hash)

        for game_hash in to_remove:
            session = games.pop(game_hash, None)
            if session:
                # Notify any remaining observers
                try:
                    await session.broadcast_stream_ended()
                except Exception:
                    pass
                print(f"[CLEANUP] Removed game {game_hash[:12]}...")


# ── Startup / main ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    host = os.getenv("HOST", "0.0.0.0")
    print(f"[START] cc-live-relay starting on {host}:{PORT}")
    print(f"[START] Buffer: {BUFFER_SECONDS}s ({BUFFER_FRAMES} frames), Max observers: {MAX_OBSERVERS_PER_GAME}")
    uvicorn.run(app, host=host, port=PORT)
