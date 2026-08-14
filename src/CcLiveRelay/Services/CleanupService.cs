using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using CcLiveRelay.Config;
using CcLiveRelay.Protocol;
using CcLiveRelay.State;
using CcLiveRelay.Util;

using CcLiveRelay.Session;
namespace CcLiveRelay.Services;

/// <summary>
/// Periodic cleanup: purge expired credentials, probe zombie sources, reap
/// ended/inactive/undescribed sessions, and run the silence demotion sweep.
///
/// Zombie-source probe: Kestrel's WebSockets KeepAliveInterval closes dead TCP
/// connections, so the probe here drops sockets Kestrel has already closed (State != Open);
/// a live but idle streamer answers the keepalive fine and stays.
/// </summary>
public sealed class CleanupService : BackgroundService
{
    private readonly RelayOptions _options;
    private readonly RelayStore _store;
    private readonly GoReporter _reporter;
    private readonly ILogger<CleanupService> _log;

    public CleanupService(RelayOptions options, RelayStore store, GoReporter reporter,
                          ILogger<CleanupService> log)
    {
        _options = options;
        _store = store;
        _reporter = reporter;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(15));
        while (await timer.WaitForNextTickAsync(ct))
        {
            try
            {
                await CleanupAsync(ct);
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                _log.LogWarning(e, "[CLEANUP] loop iteration failed");
            }
        }
    }

    private async Task CleanupAsync(CancellationToken ct)
    {
        double now = TimeSource.Now();

        // Expired credentials.
        foreach (var store in new[] { _store.WatchTickets, _store.StreamTokens })
        {
            foreach (var kv in store.ToList())
            {
                if (kv.Value.ExpiresAt < now)
                    store.TryRemove(kv.Key, out _);
            }
        }

        // Zombie-source probe: never reaps a session with connected sources, so a source
        // whose TCP connection died without a disconnect event would keep its session alive
        // forever otherwise.
        foreach (var session in _store.Games.Values.ToList())
        {
            if (session.Ended || !session.HasSources)
                continue;
            if (now - session.LastActive <= _options.InactiveGameTtl)
                continue;
            foreach (var ws in session.SourcesSnapshot)
            {
                if (!ws.IsOpen)
                {
                    _log.LogWarning("[LIVESTREAMER] [CLEANUP] Game {LobbyId}: source silent for {Seconds:F0}s " +
                                    "and socket closed — removing it", session.LobbyId, now - session.LastActive);
                    await session.RemoveSourceAsync(ws);
                }
            }
        }

        // Reap ended / inactive / undescribed sessions.
        var toRemove = new List<string>();
        var reasons = new Dictionary<string, string>();
        foreach (var (lobbyId, session) in _store.Games.ToList())
        {
            // A session with connected sources is alive even when idle: the game may be
            // paused/stalled, and reaping it would kill the watch for every observer while
            // the streamer keeps uploading into a dead session.
            bool idleWithoutSources = !session.HasSources &&
                                      now - session.LastActive > _options.InactiveGameTtl;
            if (session.Ended || idleWithoutSources)
            {
                toRemove.Add(lobbyId);
                if (!session.Ended)
                    reasons[lobbyId] = "inactivity";
                continue;
            }

            // A session nobody ever described as host is dropped once clearly abandoned —
            // but only while no source is connected (a non-host source streaming without the
            // host is alive and watchable).
            if (!session.HasLobby && !session.HasSources &&
                now - session.CreatedAt > _options.UndescribedGameTtl)
            {
                _log.LogWarning("[LIVESTREAMER] [CLEANUP] No host registration for {LobbyId} after " +
                                "{Ttl}s; dropping (host not streaming?)", lobbyId, _options.UndescribedGameTtl);
                toRemove.Add(lobbyId);
                reasons[lobbyId] = "undescribed";
            }
        }

        foreach (var lobbyId in toRemove)
        {
            _store.Games.TryRemove(lobbyId, out var session);
            if (session is null)
                continue;
            string? reason = reasons.GetValueOrDefault(lobbyId);

            // If sources are still connected when the session goes away (defensive — the
            // inactivity rule above normally prevents this), tell them, loudly, and close
            // their sockets so the client winds down instead of hanging.
            if (session.HasSources)
            {
                _log.LogWarning("[LIVESTREAMER] [CLEANUP] Game {LobbyId} removed with sources still " +
                                "connected (reason={Reason}) — notifying and closing them", lobbyId, reason ?? "ended");
                foreach (var ws in session.SourcesSnapshot)
                {
                    try
                    {
                        var reasonJson = JsonSerializer.Serialize(new { reason = reason ?? "session_ended" },
                            GameSession.JsonOpts);
                        await ws.SendAsync(BinaryEnvelope.Pack(BinaryEnvelope.MsgError,
                            Encoding.UTF8.GetBytes(reasonJson)));
                    }
                    catch (Exception)
                    {
                        // ignore
                    }
                    try
                    {
                        await ws.CloseAsync();
                    }
                    catch (Exception)
                    {
                        // ignore
                    }
                }
            }

            // A reap is a disaster, not a game end: the streamer never sent END. Tell any
            // observers still attached so they can show "stream lost" instead of finishing as
            // if the match had ended normally.
            if (reason is not null && session.ObserverCount > 0)
            {
                try
                {
                    var reasonJson = JsonSerializer.Serialize(new
                    {
                        reason,
                        msg = "relay ended the session",
                    }, GameSession.JsonOpts);
                    session.BroadcastEnvelope(BinaryEnvelope.MsgError,
                        Encoding.UTF8.GetBytes(reasonJson));
                }
                catch (Exception)
                {
                    // ignore
                }
            }
            try
            {
                session.BroadcastEnvelope(BinaryEnvelope.MsgEnd, []);
            }
            catch (Exception)
            {
                // ignore
            }
            _log.LogWarning("[LIVESTREAMER] [CLEANUP] Removed game {LobbyId} (reason={Reason}, " +
                            "sources={Sources}, observers={Observers}, body={Body}B)",
                lobbyId, reason ?? "ended", session.SourcesSnapshot.Count, session.ObserverCount, session.BodyLength);

            // A session already marked `ended` was closed by remove_source, which already
            // flagged it for an is_live=False report; a reap must flag it here.
            if (reason is not null)
                _reporter.MarkStreamEnded(lobbyId);
        }

        // Silence sweep for the all-push demotion model: a source that is silent while the
        // body advances is dead weight and should be demoted. The "never demote the last
        // active pusher" guard keeps the stream alive regardless.
        foreach (var session in _store.Games.Values.ToList())
        {
            if (session.Ended)
                continue;
            foreach (var ws in session.SourcesSnapshot)
                await session.MaybeDemoteSourceAsync(ws);
        }
    }
}
