using System.Text;
using System.Text.Json;
using CcLiveRelay.Config;
using CcLiveRelay.Session;
using CcLiveRelay.State;

namespace CcLiveRelay.Services;

/// <summary>
/// Batched livestream-state reporting to GO services (the POST /observers contract).
///
/// The relay is the only party that knows who is actually connected as a spectator and when
/// a stream truly closed, so it reports a rough estimate of that state rather than per-event
/// liveness. Every observer join/leave (including a dead-socket sweep) marks that lobby
/// dirty; the first change arms a timer, and any further changes before it fires are batched
/// into the same post (the timer is never reset). A stream becoming watchable is reported
/// immediately instead (see ReportStreamLiveAsync).
///
/// Subscribes to the store's change events, which breaks the DI cycle the other way: the
/// store cannot take a dependency on this class because this class needs the store.
/// </summary>
public sealed class GoReporter
{
    private readonly RelayOptions _options;
    private readonly RelayStore _store;
    private readonly HttpClient _http;
    private readonly ILogger<GoReporter> _log;
    private readonly object _sync = new();
    private readonly HashSet<string> _observerDirty = [];
    private readonly HashSet<string> _endedDirty = [];
    private readonly SemaphoreSlim _flushGate = new(1, 1);
    private Task? _batchTask;
    private readonly CancellationToken _shutdown;

    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNamingPolicy = null };

    public GoReporter(RelayOptions options, RelayStore store, HttpClient http,
                      ILogger<GoReporter> log, CancellationToken shutdown)
    {
        _options = options;
        _store = store;
        _http = http;
        _log = log;
        _shutdown = shutdown;
        store.ObserverCountChanged += MarkObserverChange;
        store.StreamEnded += MarkStreamEnded;
        store.StreamLive += async lobbyId =>
        {
            try
            {
                await ReportStreamLiveAsync(lobbyId);
            }
            catch (Exception e)
            {
                _log.LogWarning(e, "[LIVESTREAM] immediate live report failed");
            }
        };
    }

    public bool Enabled => !string.IsNullOrEmpty(_options.GoObserversUrl);

    /// <summary>Record that a lobby's observer set changed and arm the shared batch timer.</summary>
    public void MarkObserverChange(string lobbyId)
    {
        if (!Enabled)
            return;
        lock (_sync) _observerDirty.Add(lobbyId);
        ArmBatch();
    }

    /// <summary>
    /// Record that a lobby's stream closed and arm the shared batch timer. The relay owns
    /// stream liveness (it observes the last source leave / END / inactivity reaping), so
    /// when it closes a session it flags the lobby for an is_live=False report rather than
    /// maintaining a separate ended notification path.
    /// </summary>
    public void MarkStreamEnded(string lobbyId)
    {
        if (!Enabled)
            return;
        lock (_sync)
        {
            _endedDirty.Add(lobbyId);
            _observerDirty.Remove(lobbyId);
        }
        ArmBatch();
    }

    /// <summary>
    /// Start the shared batch timer if one is not already pending (never reset it). A change
    /// arriving mid-window must not extend the batch's fire time.
    /// </summary>
    public void ArmBatch()
    {
        lock (_sync)
        {
            if (_batchTask is { IsCompleted: false })
                return;
            _batchTask = Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(_options.ObserverChangeTimeout), _shutdown);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                lock (_sync) _batchTask = null;
                await FlushObserverBatchAsync();
            });
        }
    }

    /// <summary>
    /// Tell GO a stream became watchable, without waiting for the batch window. Observer
    /// counts and stream endings are rough estimates, and coalescing them costs nothing that
    /// matters. A stream *starting* is not in that category: GO refuses to admit an observer
    /// until it knows the stream is live. Falls back to the batch when the POST does not land.
    /// </summary>
    public async Task ReportStreamLiveAsync(string lobbyId)
    {
        if (!Enabled)
            return;
        var session = _store.GetLiveSession(lobbyId);
        // Same rule the batch applies: no header means nothing to watch yet, so not live.
        if (session is null || session.HeaderReceived == false)
            return;

        int count = session.ObserverCount;
        var entries = new List<object>
        {
            new { lobby_id = lobbyId, observer_count = count, is_live = true },
        };
        if (await NotifyLobbyProgressAsync(entries))
        {
            session.LastReportedObservers = count;
            return;
        }
        _log.LogWarning("[LIVESTREAM] [WARN] immediate live report for {LobbyId} failed; falling back to the batch", lobbyId);
        lock (_sync) _observerDirty.Add(lobbyId);
        ArmBatch();
    }

    /// <summary>
    /// Post every dirty lobby's livestream state to GO in a single request. Ended lobbies are
    /// reported with is_live=False and count 0. Nothing is committed until GO has accepted
    /// the batch; a lobby stays dirty on failure because a dropped is_live=False would strand
    /// a dead stream in GO's livestream menu permanently.
    /// </summary>
    public async Task FlushObserverBatchAsync()
    {
        if (!Enabled)
            return;
        await _flushGate.WaitAsync();
        try
        {
            HashSet<string> endedBatch;
            HashSet<string> observerBatch;
            lock (_sync)
            {
                if (_observerDirty.Count == 0 && _endedDirty.Count == 0)
                    return;
                endedBatch = [.. _endedDirty];
                observerBatch = [.. _observerDirty];
            }

            var entries = new List<object>();
            foreach (var lobbyId in endedBatch)
                entries.Add(new { lobby_id = lobbyId, observer_count = 0, is_live = false });

            var toCommit = new List<(string LobbyId, int Count)>();
            foreach (var lobbyId in observerBatch)
            {
                var session = _store.GetLiveSession(lobbyId);
                // A session with no header yet is not watchable, so it is not live: reporting
                // it would put a lobby in GO's menu that an observer can only stare at.
                if (session is null || session.HeaderReceived == false)
                {
                    lock (_sync) _observerDirty.Remove(lobbyId);
                    continue;
                }
                int count = session.ObserverCount;
                if (count == session.LastReportedObservers)
                {
                    lock (_sync) _observerDirty.Remove(lobbyId);
                    continue;
                }
                toCommit.Add((lobbyId, count));
                entries.Add(new { lobby_id = lobbyId, observer_count = count, is_live = true });
            }

            if (entries.Count == 0)
                return;

            if (!await NotifyLobbyProgressAsync(entries))
            {
                // Leave both dirty sets exactly as they are and try again on the next tick,
                // rather than dropping state GO never received.
                ArmBatch();
                return;
            }

            lock (_sync)
            {
                foreach (var lobbyId in endedBatch)
                    _endedDirty.Remove(lobbyId);
            }
            foreach (var (lobbyId, count) in toCommit)
            {
                var session = _store.GetLiveSession(lobbyId);
                if (session is not null)
                    session.LastReportedObservers = count;
                lock (_sync) _observerDirty.Remove(lobbyId);
            }
        }
        finally
        {
            _flushGate.Release();
        }
    }

    /// <summary>
    /// Tell GO services the current livestream state for a set of lobbies. Returns true only
    /// when GO accepted the batch. Never raises: GO being down or unreachable must not affect
    /// the relay's own streaming.
    /// </summary>
    public async Task<bool> NotifyLobbyProgressAsync(List<object> entries)
    {
        if (!Enabled || entries.Count == 0)
            return false;
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, _options.GoObserversUrl);
            if (!string.IsNullOrEmpty(_options.GoApiKey))
                request.Headers.TryAddWithoutValidation("X-Relay-Key", _options.GoApiKey);
            string body = JsonSerializer.Serialize(entries, JsonOpts);
            request.Content = new StringContent(body, Encoding.UTF8, "application/json");
            using var response = await _http.SendAsync(request, _shutdown);
            if ((int)response.StatusCode >= 300)
            {
                _log.LogWarning("[LIVESTREAM] [WARN] GO rejected livestream state with HTTP {Status}; will retry",
                    (int)response.StatusCode);
                return false;
            }
            return true;
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            _log.LogWarning(e, "[LIVESTREAM] [WARN] failed to notify GO livestream state");
            return false;
        }
    }
}
