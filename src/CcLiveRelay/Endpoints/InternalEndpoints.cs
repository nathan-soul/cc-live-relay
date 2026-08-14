using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CcLiveRelay.Config;
using CcLiveRelay.State;
using Microsoft.AspNetCore.Mvc;

namespace CcLiveRelay.Endpoints;

/// <summary>
/// GO-orchestration endpoints (docs/running-the-stack.md). GO services validates user JWTs
/// and calls this relay over HTTP with a shared INTERNAL_API_KEY to mint single-use stream
/// tokens (streamers) and watch tickets (observers). This relay never sees a user JWT; it
/// only trusts GO. Without INTERNAL_API_KEY the internal endpoints refuse all calls (503),
/// so a misconfigured deploy fails loudly instead of accepting unauthenticated mints.
/// </summary>
public static class InternalEndpoints
{
    private static readonly JsonSerializerOptions VerbatimJson = new() { PropertyNamingPolicy = null };

    private static bool CheckInternalKey(RelayOptions options, HttpContext context)
    {
        if (string.IsNullOrEmpty(options.InternalApiKey))
            return false;
        string supplied = context.Request.Headers["X-Relay-Key"].ToString();
        return CryptographicOperations.FixedTimeEquals(
            Encoding.UTF8.GetBytes(supplied),
            Encoding.UTF8.GetBytes(options.InternalApiKey));
    }

    /// <summary>Null when the key check passes; the error result (503/401) otherwise.</summary>
    private static IResult? RequireInternalKey(RelayOptions options, HttpContext context)
    {
        if (string.IsNullOrEmpty(options.InternalApiKey))
            return Results.Json(new { detail = "relay not configured with INTERNAL_API_KEY" },
                statusCode: StatusCodes.Status503ServiceUnavailable);
        if (!CheckInternalKey(options, context))
            return Results.Json(new { detail = "invalid or missing relay key" },
                statusCode: StatusCodes.Status401Unauthorized);
        return null;
    }

    public static void Map(WebApplication app)
    {
        app.MapGet("/health", (RelayStore store) =>
        {
            int totalObservers = store.Games.Values.Sum(g => g.ObserverCount);
            long totalBodyBytes = store.Games.Values.Sum(g => (long)g.BodyLength);
            return Results.Json(new
            {
                status = "ok",
                active_games = store.Games.Values.Count(g => !g.Ended),
                total_observers = totalObservers,
                total_body_bytes = totalBodyBytes,
            }, VerbatimJson);
        });

        app.MapPost("/internal/livestreams", async (HttpContext context, RelayStore store) =>
        {
            var options = store.Options;
            var keyError = RequireInternalKey(options, context);
            if (keyError is not null)
                return keyError;

            using var doc = await JsonDocument.ParseAsync(context.Request.Body);
            var root = doc.RootElement;
            string lobbyId = root.TryGetProperty("lobby_id", out var id) ? id.ToString() : "";
            if (lobbyId.Length == 0)
                return Results.Json(new { detail = "lobby_id required" },
                    statusCode: StatusCodes.Status400BadRequest);

            var session = store.GetOrCreateSession(lobbyId);
            session.OwnerUserId = root.TryGetProperty("owner_user_id", out var owner) &&
                                  owner.ValueKind == JsonValueKind.Number && owner.TryGetInt64(out var ownerId)
                ? ownerId
                : null;

            // The broadcast delay the host chose, forwarded by GO. Only the host's registration
            // carries one (GO sends null for every other member), so an absent value must leave
            // an already-established delay alone rather than reset it.
            if (root.TryGetProperty("delay_seconds", out var delayEl) &&
                delayEl.ValueKind != JsonValueKind.Null)
            {
                var delay = RelayStore.ParseDelaySeconds(delayEl, options.MaxDelaySeconds);
                if (delay is null)
                {
                    Console.WriteLine($"[LIVESTREAMER] [WARN] GO sent bad delay_seconds={delayEl} for " +
                                      $"lobby={lobbyId}, keeping {session.DelaySeconds}");
                }
                else
                {
                    session.DelaySeconds = delay.Value;
                    session.DelayFromGo = true;
                }
            }

            string scheme = options.PublicWsScheme;
            string host = PublicHost(options, context);
            string prefix = PublicPathPrefix(options);
            return Results.Json(new { base_url = $"{scheme}://{host}{prefix}/stream/{lobbyId}" },
                VerbatimJson);
        });

        app.MapPost("/internal/stream_tokens", (HttpContext context, RelayStore store) =>
            MintCredential(context, store, store.StreamTokens, "stream", "stream_token"));

        app.MapPost("/internal/watch_tickets", (HttpContext context, RelayStore store) =>
            MintCredential(context, store, store.WatchTickets, "watch", "ticket"));
    }

    private static async Task<IResult> MintCredential(
        HttpContext context, RelayStore store,
        System.Collections.Concurrent.ConcurrentDictionary<string, Credential> credentialStore,
        string urlPath, string queryParam)
    {
        var options = store.Options;
        var keyError = RequireInternalKey(options, context);
        if (keyError is not null)
            return keyError;

        using var doc = await JsonDocument.ParseAsync(context.Request.Body);
        var root = doc.RootElement;
        string lobbyId = root.TryGetProperty("lobby_id", out var id) ? id.ToString() : "";
        if (lobbyId.Length == 0)
            return Results.Json(new { detail = "lobby_id required" },
                statusCode: StatusCodes.Status400BadRequest);

        var session = store.GetLiveSession(lobbyId);
        if (session is null)
        {
            // The detail carries a machine-readable code so GO can tell this apart from a
            // bare routing 404 (wrong Relay.base_url, a reverse proxy that mis-handled the
            // path prefix).
            return Results.Json(new
            {
                detail = new { code = "stream_ended", message = "game not found or ended" },
            }, VerbatimJson, statusCode: StatusCodes.Status404NotFound);
        }

        long? userId = root.TryGetProperty("user_id", out var userEl) &&
                       userEl.ValueKind == JsonValueKind.Number && userEl.TryGetInt64(out var uid)
            ? uid
            : null;
        bool priority = root.TryGetProperty("priority", out var prioEl) &&
                        prioEl.ValueKind == JsonValueKind.True;

        string key = store.MintCredential(lobbyId, userId, credentialStore, priority);

        string scheme = options.PublicWsScheme;
        string host = PublicHost(options, context);
        string prefix = PublicPathPrefix(options);
        string url = $"{scheme}://{host}{prefix}/{urlPath}/{lobbyId}?{queryParam}={key}";
        return Results.Json(new { url }, VerbatimJson);
    }

    private static string PublicHost(RelayOptions options, HttpContext context) =>
        options.PublicHost.Length > 0
            ? options.PublicHost
            : (context.Request.Headers.Host.ToString() ?? context.Request.Host.Host);

    /// <summary>
    /// Normalized path prefix for public connect URLs (e.g. '/relay' or ''). The relay's own
    /// WS routes are NOT prefixed; the reverse proxy strips the prefix before forwarding, and
    /// this value only makes the minted connect URLs match the public path.
    /// </summary>
    private static string PublicPathPrefix(RelayOptions options)
    {
        var prefix = options.PublicPathPrefix.Trim();
        if (prefix.Length == 0)
            return "";
        if (!prefix.StartsWith('/'))
            prefix = "/" + prefix;
        return prefix.TrimEnd('/');
    }
}
