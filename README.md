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

### Session key: the GO lobby id
A session is keyed by the GeneralsOnline **LobbyID**, as decimal text — the same value, in the
same spelling, that GO's own `/Lobbies` JSON prints. Every player in a lobby already holds it
identically (it comes off the service, not computed locally), so no client has to derive
anything, and a relay session can be matched to a GO lobby by eye. It is also the id observers
watch by: `/watch/<lobbyid>`.

### Host authority
Every player in a lobby registers, because each is a potential source of replay bytes. Only the
**host** describes the game: the `lobby` block and `delay_seconds` are accepted from `is_host`
registrations and ignored from anyone else. Without that rule the published description of a game
was a race between eight registrations that all arrive within milliseconds of each other.

A session may be opened by whichever client connects first, host or not — rejecting non-hosts
would drop good sources purely on arrival order. Until the host describes it, the session ingests
data but is **not listed** by `GET /games`, and is dropped after `UNDESCRIBED_GAME_TTL` if the
host never registers (which is what happens when the host has streaming switched off).

### Lobby metadata shape
The `lobby` block mirrors GO's own `/lobby` response key-for-key (`lobbytype`, `region`,
`rngseed`, `mapname`, `mappath`, `name`, `owner`, `members[{userid, displayname}]`), so a client
parses the same structure whether the list came from this relay or from GO itself. The relay
allow-lists those keys: a GO lobby also carries a password, per-member ports and an anticheat id,
none of which are republished. `members[]` keeps GO's empty slots (`userid: -1`) verbatim —
filtering those is the display layer's job.

`GET /games` returns those keys flat per row, plus the relay's own `lobbyid`, `timecreated`,
`viewers`, `delay_seconds`, `age_seconds`, `body_bytes` and `sources`. Note `timecreated` is when
the **relay session** started (the relay never sees the lobby's own creation time), formatted as
ISO-8601 UTC to match GO.

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
| `UNDESCRIBED_GAME_TTL` | `120` | Seconds a session may run before the host describes it, after which it is dropped |

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
| `GET /games` | HTTP | Active games the host has described (see above) |
| `GET /debug/body/{lobbyid}` | HTTP | Inspect raw body bytes (debugging) |
| `WS /register` | WebSocket | Client registers with a binary REGISTER frame carrying `lobbyid`, `can_stream` and `is_host`; becomes a streamer or observer based on `can_stream` — streamers send HEADER/PATCH/BODY/END frames over this connection |
| `WS /watch/{lobbyid}` | WebSocket | Observer watches a game (catch-up + live stream) |
| `WS /watch-reconnect/{lobbyid}` | WebSocket | Observer reconnects with a `last_offset` hint |

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
