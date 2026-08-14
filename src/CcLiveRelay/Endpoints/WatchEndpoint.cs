using System.Net.WebSockets;
using CcLiveRelay.Protocol;
using CcLiveRelay.Session;
using CcLiveRelay.State;

namespace CcLiveRelay.Endpoints;

/// <summary>
/// WS /watch/{lobby_id} — observer connects with the single-use watch ticket GO minted.
///
/// Protocol (binary):
/// 1. Server sends ROLE config, then HEADER + BODY chunks for catch-up
/// 2. Server streams live PATCH/BODY/TICK/CHAT/END (type=2/3/9/7/4)
/// 3. Watchers may send spectator chat (type=8); nothing else is accepted from observers.
/// </summary>
public static class WatchEndpoint
{
    public static void Map(WebApplication app)
    {
        app.MapGet("/watch/{lobbyId}", async (HttpContext context, string lobbyId, RelayStore store) =>
        {
            if (!context.WebSockets.IsWebSocketRequest)
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }
            using var ws = await context.WebSockets.AcceptWebSocketAsync();
            var client = new WebSocketClientSocket(ws);
            await HandleWatchAsync(context, ws, client, lobbyId, store, context.RequestAborted);
        });
    }

    private static async Task HandleWatchAsync(HttpContext context, WebSocket ws, IClientSocket client, string lobbyId,
                                               RelayStore store, CancellationToken ct)
    {
        var options = store.Options;

        // Consume the ticket before the session check, matching /stream's ordering: a
        // single-use credential is burned on first use regardless of what check rejects it.
        string ticket = context.Request.Query["ticket"].ToString();
        var credential = store.ConsumeWatchTicket(ticket, lobbyId);
        if (credential is null)
        {
            await StreamEndpoint.RejectAsync(ws, "Missing or invalid watch ticket");
            return;
        }

        var session = store.GetLiveSession(lobbyId);
        if (session is null)
        {
            await StreamEndpoint.RejectAsync(ws, "Game not found or ended");
            return;
        }

        // GO stamps priority on the ticket for privileged watchers (admin / user_priority =
        // Viewer): they bypass the byte-level delay hold and watch the live edge.
        // Registration and catch-up are one atomic step inside AddObserver — the observer's
        // writer queue is ordered, so no live chunk can overtake its catch-up.
        if (!session.AddObserver(client, priority: credential.Priority))
        {
            await StreamEndpoint.RejectAsync(ws, "Max observers reached");
            return;
        }

        if (options.Debug)
            Console.WriteLine($"[OBSERVER] [WATCH] Observer connected to game {lobbyId} ({session.ObserverCount} viewers)");

        try
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
                while (reader.TryReadFrame(out byte msgType, out byte[] payload))
                {
                    // Watchers are senders of spectator chat only; anything else is ignored.
                    if (msgType == BinaryEnvelope.MsgSpectatorChat)
                    {
                        try
                        {
                            await session.ApplySpectatorChatAsync(client, payload);
                        }
                        catch (Exception)
                        {
                            // a single bad frame must not kill the watch loop
                        }
                    }
                }
            }
        }
        catch (WebSocketException)
        {
            if (options.Debug)
                Console.WriteLine($"[OBSERVER] [WATCH] Observer disconnected from game {lobbyId}");
        }
        catch (OperationCanceledException)
        {
            // client aborted / relay shutting down
        }
        finally
        {
            await session.RemoveObserverAsync(client);
        }
    }
}
