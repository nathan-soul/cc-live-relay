using System.Net.WebSockets;
using System.Text.Json;
using CcLiveRelay.Protocol;

using CcLiveRelay.Util;
using static CcLiveRelay.Protocol.BinaryEnvelope;
namespace CcLiveRelay.Session;

/// <summary>Per-source health for the all-push demotion model.</summary>
public sealed class SourceHealth
{
    public bool Demoted;
    public long LagBytes;
    public int GapStrikes;
    public int MismatchStrikes;
    public long FramesSeen;
    public double LastFrameAt;
    public long BodyLenSeen;
}

public sealed partial class GameSession
{
    // ── All-push demotion / re-promotion ──

    private int ActiveSourceCount()
    {
        lock (_sync)
        {
            return _sourceHealth.Count(kv => kv.Key is not null &&
                                             kv.Value is not null &&
                                             _sources.Contains(kv.Key) &&
                                             !kv.Value.Demoted);
        }
    }

    /// <summary>Whether this source has been told to stop pushing (role=backup).</summary>
    private bool _sourceDemoted(IClientSocket ws)
    {
        lock (_sync)
        {
            return _sourceHealth.TryGetValue(ws, out var health) && health.Demoted;
        }
    }

    /// <summary>
    /// A single number ordering sources worst-first: higher = worse connection. Average lag
    /// per frame is the primary signal; gap strikes weight heavily since a gappy source is
    /// actively dropping data. Used to pick the least-bad demoted source for re-promotion.
    /// </summary>
    private double _sourceHealthScore(SourceHealth health)
    {
        long frames = Math.Max(1, health.FramesSeen);
        double avgLag = health.LagBytes / (double)frames;
        return avgLag + 8 * _options.SourceLagBytes * health.GapStrikes;
    }

    /// <summary>
    /// The reason this source should be demoted, or null. Checks are cumulative over the
    /// source's lifetime rather than instantaneous, so a source must *persistently* misbehave
    /// before it is demoted. The "never demote last" rule lives in MaybeDemoteSourceAsync.
    /// </summary>
    private string? _shouldDemote(SourceHealth health)
    {
        if (health.Demoted)
            return null;
        if (health.GapStrikes >= _options.SourceGapStrikes)
            return $"gap_strikes={health.GapStrikes} >= {_options.SourceGapStrikes}";
        if (health.LagBytes >= _options.SourceLagBytes)
            return $"lag_bytes={health.LagBytes} >= {_options.SourceLagBytes}";
        double silent = TimeSource.Now() - health.LastFrameAt;
        // Silence only counts as a problem when the body has moved on without this source —
        // a paused game advances nobody's counters.
        if (silent > _options.SourceSilenceSeconds && BodyLength > health.BodyLenSeen)
            return $"silent={silent:F0}s";
        return null;
    }

    /// <summary>
    /// Demote this source if its health warrants it, never the last active pusher. Sends
    /// role=backup and marks it demoted so its frames are ignored from here on.
    /// </summary>
    public async Task<bool> MaybeDemoteSourceAsync(IClientSocket ws)
    {
        lock (_sync)
        {
            if (!_sources.Contains(ws) || _sourceDemoted(ws))
                return false;
            if (ActiveSourceCount() <= 1)
                return false;
            if (!_sourceHealth.TryGetValue(ws, out var health))
                return false;
            var reason = _shouldDemote(health);
            if (reason is null)
                return false;
            health.Demoted = true;
        }

        var roleJson = JsonSerializer.Serialize(new
        {
            role = "backup",
            lobbyid = LobbyId,
            body_offset = BodyLength,
        }, JsonOpts);
        Console.WriteLine($"[LIVESTREAMER] [DEMOTE] Game {LobbyId}: source demoted to backup");
        try
        {
            await ws.SendAsync(BinaryEnvelope.Pack(MsgRole, System.Text.Encoding.UTF8.GetBytes(roleJson)));
        }
        catch (Exception e)
        {
            Console.WriteLine($"[LIVESTREAMER] [DEMOTE] failed to notify source: {e.GetType().Name}: {e.Message}");
        }
        return true;
    }

    /// <summary>
    /// Re-promote the least-bad demoted source when no active pusher remains. Called after a
    /// source leaves. Sends a takeover ROLE carrying the current body offset so the backup can
    /// backfill from its local recording.
    /// </summary>
    public async Task<bool> MaybePromoteBackupAsync()
    {
        IClientSocket? ws = null;
        lock (_sync)
        {
            if (ActiveSourceCount() > 0)
                return false;
            KeyValuePair<IClientSocket, SourceHealth>? best = null;
            foreach (var kv in _sourceHealth)
            {
                if (!_sources.Contains(kv.Key) || !kv.Value.Demoted)
                    continue;
                if (best is null || _sourceHealthScore(kv.Value) < _sourceHealthScore(best.Value.Value))
                    best = kv;
            }
            if (best is null)
                return false;
            best.Value.Value.Demoted = false;
            ws = best.Value.Key;
        }

        var roleJson = JsonSerializer.Serialize(new
        {
            role = "streamer",
            action = "takeover",
            lobbyid = LobbyId,
            body_offset = BodyLength,
        }, JsonOpts);
        Console.WriteLine($"[LIVESTREAMER] [PROMOTE] Game {LobbyId}: backup promoted to streamer at body_offset={BodyLength}");
        try
        {
            await ws.SendAsync(BinaryEnvelope.Pack(MsgRole, System.Text.Encoding.UTF8.GetBytes(roleJson)));
        }
        catch (Exception e)
        {
            Console.WriteLine($"[LIVESTREAMER] [PROMOTE] failed to notify backup: {e.GetType().Name}: {e.Message}");
            return false;
        }
        return true;
    }
}
