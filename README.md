# cc-live-relay

Live game relay server for Command & Conquer: Generals Zero Hour, built on .NET 10.
Same wire protocol, same endpoints, same environment contract as the relay it replaces, so
the C++ LiveStreamer/LiveObserver clients and GO services treat it as a drop-in replacement.

## Architecture

```
Streamer (game client) → Relay (this server) ← GO Services (orchestrates access)
Observer  (game client)  → Relay (this server)
```

GO services validates user JWTs and calls this relay over HTTP with a shared
`INTERNAL_API_KEY` to mint **single-use stream tokens** (streamers) and **single-use watch
tickets** (observers). The relay never sees a user JWT — it only trusts GO.

## Stack

- ASP.NET Core Minimal API on Kestrel (.NET 10), no MVC, no SignalR
- Binary envelope protocol: `[1 byte type][uint32 LE length][payload]`, msg types 0-9
- `System.Text.Json` with verbatim (snake_case) property names, `BinaryPrimitives` for framing
- One bounded `Channel` per observer socket, drained by a single writer task
  (`OBSERVER_QUEUE_FRAMES` frames): broadcasts enqueue and return, so a slow observer backs up
  only its own queue and is dropped on overflow rather than stalling the source
- `BackgroundService` loops for cleanup / delay flush / GO observer reporting

## Running locally

```bash
dotnet run --project src/CcLiveRelay
# env vars are read at startup
$env:INTERNAL_API_KEY = "test123"   # PowerShell
$env:PORT = 8765
```

## Docker

```bash
# build and run
docker build -t cc-live-relay .
docker run -d --name cc-live-relay -p 8765:8765 \
  -e INTERNAL_API_KEY=test123 \
  -e GO_OBSERVERS_URL=https://go.example.com/observers \
  -e GO_API_KEY=relay-key \
  cc-live-relay

# or with compose (INTERNAL_API_KEY comes from .env)
docker compose up -d --build
docker compose logs -f relay
```

The image runs the relay on port 8765, writes replays to `/app/replays` (mount a volume
there), and healthchecks `GET /health`. Both the Dockerfile and `docker-compose.yml` use the
same environment variables (`HOST`, `PORT`, `INTERNAL_API_KEY`, `GO_API_KEY`,
`GO_OBSERVERS_URL`, `PUBLIC_HOST`, `PUBLIC_WS_SCHEME`, `PUBLIC_PATH_PREFIX`,
`MAX_OBSERVERS_PER_GAME`, `OBSERVER_QUEUE_FRAMES`, `DEFAULT_DELAY_SECONDS`, `DEBUG`).

## Configuration

Read from environment variables at startup:

| Variable | Default | Description |
|----------|---------|-------------|
| `HOST` / `PORT` | `0.0.0.0` / `8765` | Bind address / port |
| `INTERNAL_API_KEY` | (none) | GO's credential for `/internal/*`; unset → 503 on those routes |
| `GO_API_KEY` | (none) | The relay's credential for outbound GO posts (X-Relay-Key) |
| `GO_OBSERVERS_URL` | (none) | Where the batched observer state posts; empty = no reporting |
| `MAX_OBSERVERS_PER_GAME` | `1000` | Cap per session |
| `DEFAULT_DELAY_SECONDS` | `15` | Broadcast delay; clamped to [0, 600] |
| `OBSERVER_QUEUE_FRAMES` | `1024` | Per-observer send queue; overflow drops that observer |
| `SOURCE_LAG_BYTES` | `65536` | Cumulative-lag demotion threshold |
| `SOURCE_GAP_STRIKES` | `3` | Gap demotion threshold |
| `SOURCE_SILENCE_SECONDS` | `10` | Silent-while-advancing demotion threshold |
| `UNDESCRIBED_GAME_TTL` | `120` | Host-never-registered reap window |
| `WATCH_TICKET_TTL_SECONDS` | `30` | Single-use credential lifetime |
| `PUBLIC_HOST` / `PUBLIC_WS_SCHEME` / `PUBLIC_PATH_PREFIX` | — / `wss` / — | Public connect URLs |
| `OBSERVER_UPDATE_INTERVAL` / `OBSERVER_CHANGE_TIMEOUT` | `60` / `15` | GO report cadence |
| `DEBUG` | off | `1`/`true`/`yes`/`on` for verbose per-frame logging |

## Endpoints

| Endpoint | Type | Description |
|----------|------|-------------|
| `GET /health` | HTTP | Health check (unauthenticated) |
| `POST /internal/livestreams` | HTTP | GO announces a livestream; creates/reuses the session. Requires `X-Relay-Key`. Returns `{base_url}` |
| `POST /internal/stream_tokens` | HTTP | GO mints a single-use stream token. Requires `X-Relay-Key`. Returns `{url}` |
| `POST /internal/watch_tickets` | HTTP | GO mints a single-use watch ticket. Requires `X-Relay-Key`. Returns `{url}` |
| `WS /stream/{lobbyid}?stream_token=KEY` | WebSocket | Streamer connects (REGISTER → ROLE → HEADER/PATCH/BODY/TICK/CHAT/END) |
| `WS /watch/{lobbyid}?ticket=KEY` | WebSocket | Observer watches (catch-up + live stream) |

Behaviour notes: watch tickets and stream tokens are
single-use and short-lived; a lobby is only listed as live once its host's replay header
arrives; observers of a delayed stream are byte-held behind the delay and never receive
`MSG_TICK`; a `delay_seconds` from GO wins over the host's REGISTER fallback; replays are
saved to `replays/{lobby_id}.rep` when a stream ends.

## Testing

```bash
dotnet test CcLiveRelay.slnx
```

35 tests covering: session delivery semantics (exactly-once body, watermark, hold, tick
rules, demotion/re-promotion), GO batching (debounce, retry, baseline, immediate live
report), ticket auth, and real-socket flows over the in-process TestServer.

## Layout

```
src/CcLiveRelay/
  Program.cs                  — bootstrap, DI, route mapping
  Config/RelayOptions.cs      — env config
  Protocol/                   — BinaryEnvelope, EnvelopeReader (frame reassembly)
  Util/                       — ByteBuffer, TimeSource (clock seam), TokenGenerator
  State/                      — RelayStore (sessions/credentials), Credential
  Session/                    — GameSession + health/chat/observers/broadcast partials
  Endpoints/                  — /internal/*, /stream, /watch
  Services/                   — GoReporter, cleanup, delay flush, observer report, shutdown
tests/CcLiveRelay.Tests/      — xUnit suites
```
