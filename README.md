# cc-live-relay

Live game relay server for Command & Conquer: Generals Zero Hour.

## Architecture

```
Streamer (game client) → Relay (this server) ← GO Services (orchestrates access)
Observer  (web client)  → Relay (this server)
```

GO Services validates user JWTs and calls this relay over HTTP with a shared
`INTERNAL_API_KEY` to mint **single-use stream tokens** (for streamers) and **single-use
watch tickets** (for observers). The relay never sees a user JWT — it only trusts GO. See
`plans/relay-go-orchestrated-livestreams.md` for the full design.

## Protocols

### Session key: the GO lobby id
A session is keyed by the GeneralsOnline **LobbyID**, as decimal text — the same value, in the
same spelling, that GO's own `/Lobbies` JSON prints. Every player in a lobby already holds it
identically (it comes off the service, not computed locally), so no client has to derive
anything, and a relay session can be matched to a GO lobby by eye. It is also the id observers
watch by: `/watch/<lobbyid>`. This is the same id GO uses when it mints stream tokens and watch
tickets, so the mapping is direct.

### Host authority
Every player in a lobby is a potential source of replay bytes. Only the **host** describes the
game: the `lobby` block and `delay_seconds` are accepted from `is_host` registrations and ignored
from anyone else. Without that rule the published description of a game was a race between eight
registrations that all arrive within milliseconds of each other.

A session may be opened by whichever client connects first, host or not — rejecting non-hosts
would drop good sources purely on arrival order. Until the host describes it, the session ingests
data but is dropped after `UNDESCRIBED_GAME_TTL` if the host never registers (which is what
happens when the host has streaming switched off).

### Lobby metadata shape
The `lobby` block mirrors GO's own `/lobby` response key-for-key (`lobbytype`, `region`,
`rngseed`, `mapname`, `mappath`, `name`, `owner`, `members[{userid, displayname}]`), so a client
parses the same structure whether the list came from this relay or from GO itself. The relay
allow-lists those keys: a GO lobby also carries a password, per-member ports and an anticheat id,
none of which are republished. `members[]` keeps GO's empty slots (`userid: -1`) verbatim —
filtering those is the display layer's job.

### Tickets are single-use
Every stream token and watch ticket is consumed on first use and cannot be reused. This matches
the game's own rule that you cannot rejoin a game after leaving — a dropped connection requires
the client to get a fresh credential from GO.

## Configuration

Via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `RELAY_HOST` | `0.0.0.0` | Bind address |
| `RELAY_PORT` | `8765` | Listen port |
| `DEBUG` | off | Set to `1`/`true`/`yes`/`on` for verbose per-game/per-connection logging |
| `UNDESCRIBED_GAME_TTL` | `120` | Seconds a session may run before the host describes it, after which it is dropped |
| `INTERNAL_API_KEY` | (none) | **GO's credential** to call this relay: GO sends this as `X-Relay-Key` on every `/internal/*` call (matches GO's `Relay.api_key`). Without it the internal endpoints refuse all calls (503) |
| `GO_API_KEY` | (none) | **The relay's own credential** for outbound calls to GO (the stream-ended notification), sent as `X-Relay-Key` (matches GO's `Relay.ingress_api_key`). Distinct from `INTERNAL_API_KEY` so each side has its own secret |
| `WATCH_TICKET_TTL_SECONDS` | `30` | Lifetime for a minted credential. GO does not send an expiry — the client mints and connects immediately, so a short fixed window is plenty (and keeps the stolen-ticket replay window small). On reconnect, a fresh ticket is minted |
| `GO_OBSERVERS_URL` | (none) | Optional. Where the relay POSTs the batched livestream state: `[{"lobby_id": "...", "observer_count": n, "is_live": bool}, ...]` for every lobby whose observer count changed or whose stream ended. Sent with `GO_API_KEY` as `X-Relay-Key`. Empty = relay never reports to GO |
| `OBSERVER_UPDATE_INTERVAL` | `60` | Seconds between periodic observer-count reports to GO for every active game, as a baseline even when nobody joins/leaves |
| `OBSERVER_CHANGE_TIMEOUT` | `15` | Seconds after the first observer join/leave before the relay posts the count to GO. Changes that arrive before the timer fires are batched into that single post — the timer is never reset, so the report always goes out `OBSERVER_CHANGE_TIMEOUT` seconds after the first change. The count is a rough estimate, not per-event liveness |
| `PUBLIC_HOST` | (request host) | Public host used when building connect URLs, if the request's own Host header isn't right |
| `PUBLIC_WS_SCHEME` | `wss` | Scheme used when building the connect URLs |

## Starting

```bash
pip install -r requirements.txt
INTERNAL_API_KEY=test123 python server.py
# or
RELAY_PORT=8765 INTERNAL_API_KEY=test123 python server.py
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
| `GET /health` | HTTP | Health check (unauthenticated) |
| `POST /internal/livestreams` | HTTP | GO announces a livestream; creates/reuses the session. Requires `X-Relay-Key`. Returns `{base_url}` |
| `POST /internal/stream_tokens` | HTTP | GO mints a single-use stream token for one lobby member. Requires `X-Relay-Key`. Returns `{url}` |
| `POST /internal/watch_tickets` | HTTP | GO mints a single-use watch ticket for one observer. Requires `X-Relay-Key`. Returns `{url}` |
| `DELETE /internal/livestreams/{lobbyid}` | HTTP | GO reports the match ended; ends and reaps the session. Requires `X-Relay-Key` |
| `WS /stream/{lobbyid}?stream_token=KEY` | WebSocket | Streamer connects with a single-use stream token, then sends REGISTER/HEADER/PATCH/BODY/END frames |
| `WS /watch/{lobbyid}?ticket=KEY` | WebSocket | Observer watches a game (catch-up + live stream) with a single-use watch ticket |

### Livestream state to GO

The relay is the only party that knows who is actually connected as a spectator and when a
stream truly closed, so it reports a **rough estimate** of that state to GO rather than
per-event liveness. Every observer join/leave (including a dead-socket sweep) marks that
lobby dirty; the first change arms a timer, and any further changes before it fires are
batched into the same post (the timer is never reset). When it fires, **all** dirty lobbies
are sent to GO in a single request:

```json
[{"lobby_id": "123", "observer_count": 4, "is_live": true}]
```

A lobby whose stream ended (last source gone / END / inactivity reap / DELETE) is sent with
`is_live: false` and a count of `0`, which is how GO stops listing it — there is no separate
"stream ended" notification. Separately, every `OBSERVER_UPDATE_INTERVAL` seconds the same
dirty sets are flushed as a baseline, so a static stream's count never goes stale. Lobbies
whose count is unchanged from the last posted value are skipped.

### Retired endpoints
`GET /games`, `GET /debug/body/{lobbyid}`, `GET /watch/{lobbyid}/ticket`,
`WS /register` and `WS /watch-reconnect/{lobbyid}` are removed — the live-games menu and
watch/stream admission are owned by GO Services.

## Testing

```bash
pip install -r requirements-dev.txt

# Integration tests against a live server (needs a running relay on RELAY_TEST_PORT):
python server.py
python test_relay.py

# GO-orchestrated ticket flow, fully mocked (no real GO services call, no real sockets —
# safe to run anywhere; uses INTERNAL_API_KEY=test123 by default):
python test_relay_auth_mock.py

# GameSession delivery semantics, no server required:
python test_session_unit.py

# Observer-count reporting (debounce + periodic), no server required:
python test_observer_report.py
```

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
