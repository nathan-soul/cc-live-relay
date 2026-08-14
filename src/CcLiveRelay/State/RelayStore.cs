using System.Collections.Concurrent;
using System.Text.Json;
using CcLiveRelay.Config;
using CcLiveRelay.Session;
using CcLiveRelay.Util;

namespace CcLiveRelay.State;

/// <summary>
/// The relay's in-process state: active sessions plus the single-use credentials minted on
/// GO's behalf. In-process for now — becomes Redis once a dispatcher tier shares the same
/// lookups across processes in a future multi-node deployment.
///
/// GO-reporter wiring: GoReporter depends on this store, and GameSession needs to reach the
/// reporter — a constructor cycle in DI. The store therefore exposes events that the reporter
/// subscribes to; sessions never reference the reporter directly. With no subscriber (unit
/// tests) the hooks are no-ops, which matches the behaviour when GO reporting is
/// disabled (empty GO_OBSERVERS_URL).
/// </summary>
public sealed class RelayStore
{
    private readonly object _sync = new();
    private readonly RelayOptions _options;

    public ConcurrentDictionary<string, GameSession> Games { get; } = new();
    public ConcurrentDictionary<string, Credential> WatchTickets { get; } = new();
    public ConcurrentDictionary<string, Credential> StreamTokens { get; } = new();

    /// <summary>An observer joined/left this session (coalesced count report).</summary>
    public event Action<string>? ObserverCountChanged;
    /// <summary>The stream for this session closed (is_live=false report).</summary>
    public event Action<string>? StreamEnded;
    /// <summary>A stream became watchable — reported immediately, not batched.</summary>
    public event Func<string, Task>? StreamLive;

    public RelayStore(RelayOptions options)
    {
        _options = options;
    }

    public RelayOptions Options => _options;

    /// <summary>Return the live session for a lobby, creating a fresh one if none exists.</summary>
    public GameSession GetOrCreateSession(string lobbyId)
    {
        lock (_sync)
        {
            if (!Games.TryGetValue(lobbyId, out var session) || session.Ended)
            {
                session = new GameSession(lobbyId, _options, this);
                Games[lobbyId] = session;
            }
            return session;
        }
    }

    /// <summary>Null unless a live (not ended) session exists for this lobby.</summary>
    public GameSession? GetLiveSession(string lobbyId)
    {
        return Games.TryGetValue(lobbyId, out var session) && !session.Ended ? session : null;
    }

    public string MintCredential(string lobbyId, long? userId,
                                 ConcurrentDictionary<string, Credential> store, bool priority)
    {
        var key = TokenGenerator.TokenUrlSafe(24);
        store[key] = new Credential
        {
            LobbyId = lobbyId,
            UserId = userId,
            Priority = priority,
            ExpiresAt = TimeSource.Now() + _options.WatchTicketTtlSeconds,
        };
        return key;
    }

    /// <summary>
    /// Validate and burn a single-use credential. Pops unconditionally (even on a lobby_id
    /// mismatch) so a credential is single-use regardless of which check fails it — a client
    /// can't retry a stolen/mismatched credential against a different lobby after a first
    /// failed attempt.
    /// </summary>
    public Credential? ConsumeCredential(string? key, string lobbyId,
                                         ConcurrentDictionary<string, Credential> store)
    {
        if (string.IsNullOrEmpty(key))
            return null;
        if (!store.TryRemove(key, out var cred))
            return null;
        if (cred.LobbyId != lobbyId || cred.ExpiresAt < TimeSource.Now())
            return null;
        return cred;
    }

    public Credential? ConsumeWatchTicket(string? key, string lobbyId) =>
        ConsumeCredential(key, lobbyId, WatchTickets);

    public Credential? ConsumeStreamToken(string? key, string lobbyId) =>
        ConsumeCredential(key, lobbyId, StreamTokens);

    public void NotifyObserverChange(string lobbyId) => ObserverCountChanged?.Invoke(lobbyId);

    public void NotifyStreamEnded(string lobbyId) => StreamEnded?.Invoke(lobbyId);

    public Task NotifyStreamLive(string lobbyId) =>
        StreamLive?.Invoke(lobbyId) ?? Task.CompletedTask;

    // ── GO-shaped lobby metadata ────────────────────────────────────────────
    //
    // The client sends the descriptive half of its GeneralsOnline lobby verbatim under
    // "lobby" in REGISTER, using GO's own key spelling, and the relay republishes it. The
    // allow-lists are the point: a GO lobby also carries a password, per-member ports and an
    // anticheat id, none of which are a third-party viewer's business.
    private static readonly string[] LobbyKeys =
        ["lobbytype", "region", "rngseed", "mapname", "mappath", "name", "owner"];
    private static readonly string[] LobbyMemberKeys = ["userid", "displayname"];

    /// <summary>Reduce a client-sent lobby block to the allow-listed keys. Never raises.</summary>
    public static Dictionary<string, object?> SanitizeLobby(JsonElement raw)
    {
        var lobby = new Dictionary<string, object?>();
        if (raw.ValueKind != JsonValueKind.Object)
            return lobby;
        foreach (var key in LobbyKeys)
            if (raw.TryGetProperty(key, out var value))
                lobby[key] = JsonToObject(value);

        var members = new List<object?>();
        if (raw.TryGetProperty("members", out var membersEl) &&
            membersEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var member in membersEl.EnumerateArray())
            {
                if (member.ValueKind != JsonValueKind.Object)
                    continue;
                var dict = new Dictionary<string, object?>();
                foreach (var key in LobbyMemberKeys)
                    if (member.TryGetProperty(key, out var value))
                        dict[key] = JsonToObject(value);
                members.Add(dict);
            }
        }
        lobby["members"] = members;
        return lobby;
    }

    private static object? JsonToObject(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.String => element.GetString(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.Number => element.GetDouble(),
        JsonValueKind.Null => null,
        _ => element.GetRawText(),
    };

    /// <summary>Display names of the occupied slots only.</summary>
    public static List<string> LobbyPlayerNames(Dictionary<string, object?> lobby)
    {
        var names = new List<string>();
        if (!lobby.TryGetValue("members", out var membersObj) ||
            membersObj is not List<object?> members)
            return names;
        foreach (var memberObj in members)
        {
            if (memberObj is not Dictionary<string, object?> member)
                continue;
            if (member.TryGetValue("userid", out var uid) && uid is double d && d == -1)
                continue;
            if (member.TryGetValue("displayname", out var name) && name is string s &&
                s.Length > 0 && !names.Contains(s))
                names.Add(s);
        }
        return names;
    }

    /// <summary>
    /// Clamp a supplied broadcast delay into [0, maxDelay]. Null if unusable. Shared by both
    /// sources of the value — GO on /internal/livestreams and the host's REGISTER frame — so
    /// a delay is bounded identically no matter which path it arrived on.
    /// </summary>
    public static int? ParseDelaySeconds(JsonElement raw, int maxDelay)
    {
        try
        {
            int value = raw.ValueKind switch
            {
                JsonValueKind.Number => raw.GetInt32(),
                JsonValueKind.String => int.Parse(raw.GetString()!, System.Globalization.CultureInfo.InvariantCulture),
                _ => throw new FormatException(),
            };
            return Math.Max(0, Math.Min(value, maxDelay));
        }
        catch (Exception)
        {
            return null;
        }
    }

    public static int? ParseDelaySeconds(object? raw, int maxDelay)
    {
        try
        {
            int value = Convert.ToInt32(raw, System.Globalization.CultureInfo.InvariantCulture);
            return Math.Max(0, Math.Min(value, maxDelay));
        }
        catch (Exception)
        {
            return null;
        }
    }
}
