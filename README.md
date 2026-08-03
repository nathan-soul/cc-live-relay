# cc-live-relay

Live game relay server for Command & Conquer: Generals Zero Hour.

## Architecture

```
Streamer (game client) → Relay (this server) → Observer (web client)
```

The relay server receives game data from a streamer (a player in the game)
and forwards it to observers (viewers). The streamer decides who is allowed
to stream (out-of-band selection).

## Protocols

### Streamer selection (out-of-band)
The streamer is selected by the relay server, not by the game.
This prevents players from seeing each other in-game.

### Game hash
Every game gets a deterministic hash:
```
SHA256(map|mode|start_time|sorted_players)
```
All clients have the same data, so the same hash.

### Failover
When the streamer disconnects, a backup client takes over.
The relay detects this automatically.

## Configuration

Via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `RELAY_HOST` | `0.0.0.0` | Bind address |
| `RELAY_PORT` | `8765` | Listen port |
| `DEBUG` | off | Set to `1`/`true`/`yes`/`on` for verbose per-game/per-connection logging |

## Starting

```bash
pip install -r requirements.txt
python server.py
# or
RELAY_PORT=8765 python server.py
```

Or use the start script (creates an isolated `.venv`, installs dependencies,
sets `HOST`/`PORT`, and opens/closes the TCP port via the firewall —
firewalld, ufw or iptables):

```bash
./start-relay.sh
# or with custom host/port
./start-relay.sh 8765 0.0.0.0
```

The port is opened before the relay starts and closed again automatically
when the relay exits or crashes. Pre-existing firewall rules are left
untouched. Firewall commands run through `sudo` (only when not already root);
the relay itself runs as your normal user.

## Endpoints

All endpoints are served on the same port. Streamers and observers connect to
the same host/port, only differing by path.

| Endpoint | Type | Description |
|----------|------|-------------|
| `GET /health` | HTTP | Health check |
| `GET /games` | HTTP | List of active games |
| `GET /debug/body/{game_id}` | HTTP | Inspect raw body bytes (debugging) |
| `WS /register` | WebSocket | Client registers with a binary REGISTER frame; becomes a streamer or observer based on `can_stream` — streamers send HEADER/PATCH/BODY/END frames over this connection |
| `WS /watch/{game_id}` | WebSocket | Observer watches a game (catch-up + live stream) |
| `WS /watch-reconnect/{game_id}` | WebSocket | Observer reconnects with a `last_offset` hint |

## Development

This is a skeleton — the basic structure is in place, but the full
implementation (frame buffer, failover, command serialization) comes later.

### Frame buffer
- Ring buffer of 900 frames (30 seconds × 30 fps)
- Observers can "seek" to earlier frames
- Stored in memory (later: Redis/disk)

### Command serialization
- Based on the Recorder::writeToFile format
- Reuses the same binary protocol as the original game
