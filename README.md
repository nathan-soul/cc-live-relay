# cc-live-relay

Live game relay server voor Command & Conquer: Generals Zero Hour.

## Architectuur

```
Streamer (game client) → Relay (deze server) → Observer (web client)
```

De relay server ontvangt game data van een streamer (speler in het spel)
en stuurt deze door naar observers (kijkers). De streamer bepaalt wie
er mag streamen (out-of-band selectie).

## Protocollen

### Streamer selectie (out-of-band)
De streamer wordt geselecteerd door de relay server, niet door het spel.
Dit voorkomt dat spelers elkaar kunnen zien in-game.

### Game hash
Elke game krijgt een deterministische hash:
```
SHA256(map|mode|start_time|sorted_players)
```
Alle clients hebben dezelfde data, dus dezelfde hash.

### Failover
Bij disconnect van de streamer neemt een backup client over.
De relay detecteert dit automatisch.

## Configuratie

Via environment variables:

| Variabel | Default | Beschrijving |
|----------|---------|--------------|
| `RELAY_HOST` | `0.0.0.0` | Bind adres |
| `RELAY_PORT` | `8765` | Luister poort |

## Starten

```bash
pip install -r requirements.txt
python server.py
# of
RELAY_PORT=8765 python server.py
```

## WebSocket endpoints

| Endpoint | Type | Beschrijving |
|----------|------|--------------|
| `GET /health` | HTTP | Health check |
| `GET /games` | HTTP | Lijst actieve games |
| `WS /register` | WebSocket | Streamer registreert zich |
| `WS /stream/{game_id}` | WebSocket | Streamer stuurt frames |
| `WS /watch/{game_id}` | WebSocket | Observer kijkt mee |

## Development

Dit is een skeleton — de basisstructuur staat, maar de volledige
implementatie (frame buffer, failover, command serialisatie) komt later.

### Frame buffer
- Ring buffer van 900 frames (30 seconden × 30 fps)
- Observers kunnen "seeken" naar eerdere frames
- Wordt opgeslagen in geheugen (later: Redis/disk)

### Command serialisatie
- Gebaseerd op Recorder::writeToFile formaat
- Hergebruikt hetzelfde binary protocol als de originele game
