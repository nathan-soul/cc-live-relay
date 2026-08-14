using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using CcLiveRelay.Protocol;
using CcLiveRelay.Session;
using CcLiveRelay.State;

namespace CcLiveRelay.Endpoints;

/// <summary>
/// WS /stream/{lobby_id} — streamer connects with the single-use stream_token GO minted.
///
/// Protocol (binary):
/// 1. Client sends REGISTER frame (type=0), payload = JSON with lobbyid/can_stream/player_name
/// 2. Server sends ROLE frame (type=5), payload = JSON {"role":"streamer","lobbyid":"..."}
/// 3. Source sends HEADER (type=1), then PATCH/BODY/TICK/CHAT/END (type=2/3/9/7/4)
/// </summary>
public static class StreamEndpoint
{
    public static void Map(WebApplication app)
    {
        app.MapGet("/stream/{lobbyId}", async (HttpContext context, string lobbyId, RelayStore store) =>
        {
            if (!context.WebSockets.IsWebSocketRequest)
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }
            using var ws = await context.WebSockets.AcceptWebSocketAsync();
            var client = new WebSocketClientSocket(ws);
            await HandleStreamAsync(context, ws, client, lobbyId, store, context.RequestAborted);
        });
    }

    private static async Task HandleStreamAsync(HttpContext context, WebSocket ws, IClientSocket client, string lobbyId,
                                                RelayStore store, CancellationToken ct)
    {
        var options = store.Options;
        GameSession? session = null;
        string role = "unknown";

        string token = context.Request.Query["stream_token"].ToString();
        var credential = store.ConsumeStreamToken(token, lobbyId);
        if (credential is null)
        {
            await RejectAsync(ws, "Invalid or expired stream token");
            return;
        }

        try
        {
            // ── Receive REGISTER frame (binary) ────────────────────────────
            var reader = new EnvelopeReader();
            byte[] buffer = new byte[64 * 1024];
            WebSocketReceiveResult receive = await ws.ReceiveAsync(buffer, ct);
            if (receive.MessageType != WebSocketMessageType.Binary || receive.Count == 0)
            {
                await RejectAsync(ws, "Expected binary REGISTER frame");
                return;
            }
            reader.Append(buffer.AsMemory(0, receive.Count));
            if (!reader.TryReadFrame(out byte msgType, out byte[] payload) ||
                msgType != BinaryEnvelope.MsgRegister || payload.Length == 0)
            {
                await RejectAsync(ws, "Expected REGISTER message (type=0)");
                return;
            }

            JsonDocument reg = JsonDocument.Parse(Encoding.UTF8.GetString(payload));
            using (reg)
            {
                var root = reg.RootElement;
                string playerName = GetString(root, "player_name", "unknown");
                bool canStream = GetBool(root, "can_stream", false);
                // In-game observer (Side 1, not an active player)? Gates which sources receive
                // spectator chat.
                bool isObserver = GetBool(root, "is_observer", false);

                if (!canStream)
                {
                    await RejectAsync(ws, "stream token valid, but can_stream required");
                    return;
                }

                // ── Assign session ───────────────────────────────────────
                // The session is keyed by the URL lobby_id, which is GO's LobbyID (the value
                // GO minted the stream token for). A REGISTER payload carrying a different
                // lobbyid is ignored — the URL is the authority here.
                session = store.GetOrCreateSession(lobbyId);

                // ── Host-authoritative fields ─────────────────────────────
                // Host authority is GO's to grant, not the client's to claim. GO records the
                // lobby's owner on the session and mints each stream token against a specific
                // user id, so the two are compared here instead of trusting an is_host flag.
                bool isHost = session.OwnerUserId is long ownerId && credential.UserId == ownerId;

                if (isHost)
                {
                    if (root.TryGetProperty("lobby", out var lobbyEl))
                    {
                        var lobby = RelayStore.SanitizeLobby(lobbyEl);
                        if (lobby.Count > 0)
                        {
                            session.Lobby = lobby;
                            if (options.Debug)
                                Console.WriteLine($"[LIVESTREAMER] [REGISTER] host described lobby for {session.LobbyId}");
                        }
                    }

                    // Fallback only: GO forwards the host's delay on /internal/livestreams
                    // before any source connects, and that value wins.
                    if (root.TryGetProperty("delay_seconds", out var delayEl) &&
                        delayEl.ValueKind != JsonValueKind.Null &&
                        !session.DelayFromGo)
                    {
                        var delay = RelayStore.ParseDelaySeconds(delayEl, options.MaxDelaySeconds);
                        if (delay is null)
                        {
                            Console.WriteLine($"[LIVESTREAMER] [WARN] bad delay_seconds={delayEl}, " +
                                              $"keeping {session.DelaySeconds}");
                        }
                        else
                        {
                            session.DelaySeconds = delay.Value;
                            if (options.Debug)
                                Console.WriteLine($"[LIVESTREAMER] [DELAY] Game {session.LobbyId}: " +
                                                  $"delay_seconds={delay.Value} (from REGISTER)");
                        }
                    }
                }

                role = "streamer";
                lock (session.Sync)
                {
                    session.AddSource(client, isObserver, session.BodyLength);
                }

                // ── Send ROLE response (binary) ──────────────────────────
                var roleJson = JsonSerializer.Serialize(new
                {
                    role,
                    lobbyid = session.LobbyId,
                    body_offset = session.BodyLength,
                }, GameSession.JsonOpts);
                await ws.SendAsync(BinaryEnvelope.Pack(BinaryEnvelope.MsgRole, Encoding.UTF8.GetBytes(roleJson)),
                    WebSocketMessageType.Binary, true, ct);
                if (options.Debug)
                    Console.WriteLine($"[LIVESTREAMER] [REGISTER] {playerName} -> role={role} game={session.LobbyId}");

                await SourceLoopAsync(context, ws, client, session, ct);
            }
        }
        catch (WebSocketException)
        {
            if (options.Debug)
                Console.WriteLine($"[LIVESTREAMER] [DISCONNECT] Client disconnected (role={role})");
        }
        catch (OperationCanceledException)
        {
            // client aborted / relay shutting down
        }
        catch (Exception e)
        {
            Console.WriteLine($"[LIVESTREAMER] [ERROR] /stream error: {e}");
            try
            {
                await ws.SendAsync(
                    BinaryEnvelope.Pack(BinaryEnvelope.MsgError,
                        Encoding.UTF8.GetBytes($"Internal error: {e.Message}")),
                    WebSocketMessageType.Binary, true, CancellationToken.None);
            }
            catch (Exception)
            {
                // ignore
            }
        }
        finally
        {
            if (session is not null && role == "streamer")
                await session.RemoveSourceAsync(client);
        }
    }

    /// <summary>Receive binary frames (HEADER/PATCH/BODY/TICK/CHAT/END) from a source.</summary>
    private static async Task SourceLoopAsync(HttpContext context, WebSocket ws, IClientSocket client,
                                              GameSession session, CancellationToken ct)
    {
        var reader = new EnvelopeReader();
        byte[] buffer = new byte[64 * 1024];
        while (true)
        {
            WebSocketReceiveResult receive = await ws.ReceiveAsync(buffer, ct);
            if (receive.MessageType == WebSocketMessageType.Close)
                break;
            if (receive.MessageType != WebSocketMessageType.Binary)
                continue;
            reader.Append(buffer.AsMemory(0, receive.Count));

            bool endReceived = false;
            while (reader.TryReadFrame(out byte msgType, out byte[] payload))
            {
                switch (msgType)
                {
                    case BinaryEnvelope.MsgHeader:
                        await session.ApplyHeaderAsync(client, payload);
                        break;
                    case BinaryEnvelope.MsgPatch:
                        await session.ApplyPatchAsync(client, payload);
                        break;
                    case BinaryEnvelope.MsgBody:
                        await session.ApplyBodyAsync(client, payload);
                        break;
                    case BinaryEnvelope.MsgTick:
                        await session.ApplyTickAsync(client, payload);
                        break;
                    case BinaryEnvelope.MsgChat:
                        await session.ApplyChatAsync(client, payload);
                        break;
                    case BinaryEnvelope.MsgSpectatorChat:
                        // Defence in depth: a streaming player may not send spectator chat; an
                        // observer-mode source may.
                        if (session.IsObserverSource(client))
                            await session.ApplySpectatorChatAsync(client, payload);
                        break;
                    case BinaryEnvelope.MsgEnd:
                        session.MarkEndReceived();
                        endReceived = true;
                        break;
                }
                if (endReceived)
                    break;
            }
            if (endReceived)
                break;

            session.TouchSource(client);
            // Demotion is checked per-frame so a persistently bad source is stopped quickly.
            if (await session.MaybeDemoteSourceAsync(client))
                Console.WriteLine($"[LIVESTREAMER] [DEMOTE] Source no longer pushing for game {session.LobbyId}");
        }
    }

    internal static async Task RejectAsync(WebSocket ws, string message)
    {
        try
        {
            await ws.SendAsync(
                BinaryEnvelope.Pack(BinaryEnvelope.MsgError, Encoding.UTF8.GetBytes(message)),
                WebSocketMessageType.Binary, true, CancellationToken.None);
        }
        catch (Exception)
        {
            // ignore
        }
        try
        {
            await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, null, CancellationToken.None);
        }
        catch (Exception)
        {
            // ignore
        }
    }

    internal static string GetString(JsonElement root, string key, string fallback) =>
        root.TryGetProperty(key, out var el) && el.ValueKind == JsonValueKind.String
            ? el.GetString() ?? fallback
            : fallback;

    internal static bool GetBool(JsonElement root, string key, bool fallback) =>
        root.TryGetProperty(key, out var el) &&
        (el.ValueKind == JsonValueKind.True || el.ValueKind == JsonValueKind.False)
            ? el.GetBoolean()
            : fallback;
}
