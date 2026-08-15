using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using CcLiveRelay.Config;
using CcLiveRelay.Protocol;
using CcLiveRelay.State;
using CcLiveRelay.Util;

using System.Buffers.Binary;
using static CcLiveRelay.Protocol.BinaryEnvelope;
namespace CcLiveRelay.Session;

/// <summary>
/// One active game: multiple sources, multiple observers. Partial classes split the port by
/// concern: ingestion + lifecycle here, demotion in SourceHealth, chat in Chat, observers
/// + delay hold in Observers, fan-out in Broadcast.
///
/// Concurrency: every mutation runs under <c>_sync</c> (never held across an await), and each
/// observer has one ObserverWriter — a bounded channel drained by a single writer task — so
/// per-observer order is exact and no send is ever awaited on the source loop. Catch-up is
/// enqueued atomically with registration, so it can never be overtaken by a live chunk.
/// </summary>
public sealed partial class GameSession
{
    private readonly object _sync = new();
    private readonly RelayOptions _options;
    private readonly RelayStore _store;

    /// <summary>GO's LobbyID as decimal text — the relay's session key.</summary>
    public string LobbyId { get; }

    // ── Descriptive / config state (all guarded by _sync) ──────────────────
    private Dictionary<string, object?> _lobby = new();
    private readonly double _createdAt = TimeSource.Now();
    private double _lastActive = TimeSource.Now();
    private int _delaySeconds;
    private bool _delayFromGo;
    private long? _ownerUserId;
    private int? _lastReportedObservers;

    // ── Replay data (guarded by _sync) ──────────────────────────────────────
    private readonly ByteBuffer _header = new();
    private bool _headerReceived;
    private readonly ByteBuffer _body = new();
    private bool _ended;
    private bool _endReceived;

    // ── Sockets and per-socket state (guarded by _sync) ─────────────────────
    private readonly HashSet<IClientSocket> _sources = [];
    private readonly HashSet<IClientSocket> _observerWsSet = [];
    private readonly Dictionary<IClientSocket, SourceHealth> _sourceHealth = [];
    private readonly Dictionary<IClientSocket, bool> _sourceObserver = [];
    private readonly Dictionary<IClientSocket, List<double>> _spectatorRate = [];
    private readonly Dictionary<IClientSocket, ObserverWriter> _observerWriters = [];
    private readonly Dictionary<IClientSocket, long> _observerCatchupLimit = [];
    private readonly Dictionary<IClientSocket, bool> _observerHeld = [];

    // ── Shared state (guarded by _sync) ─────────────────────────────────────
    /// <summary>Newest frame heartbeat seen from any source. Monotonic.</summary>
    private uint _lastTickFrame;

    /// <summary>Latest telemetry payload from the source, [logicFps u32][pingMs u32], or empty
    /// before the first one. Not monotonic in any sense - the values move both ways - so unlike
    /// the tick this is simply the last value seen.</summary>
    private byte[] _lastStats = [];

    public GameSession(string lobbyId, RelayOptions options, RelayStore store)
    {
        LobbyId = lobbyId;
        _options = options;
        _store = store;
        _delaySeconds = options.DefaultDelaySeconds;
    }

    // ── Locked accessors for other classes (cleanup loop, GO reporter) ──────
    /// <summary>Public handle on the session lock, for endpoint-side atomic registration.</summary>
    public object Sync => _sync;

    /// <summary>
    /// Queue one frame for an observer. Must be called while holding <c>_sync</c>: enqueues are
    /// serialized there, which makes each observer's delivery order exact. A full queue means
    /// the observer cannot keep up — it is dropped (rejoin re-serves catch-up from the body).
    /// </summary>
    private bool EnqueueObserverFrame(IClientSocket ws, byte[] frame)
    {
        if (_observerWriters.TryGetValue(ws, out var writer) && writer.TryEnqueue(frame))
            return true;
        return false;
    }

    /// <summary>
    /// Called by an ObserverWriter when its observer dies (send failure / queue overflow).
    ///
    /// Reports the count change exactly like the ordinary leave path: the watch loop wakes on
    /// the closed socket and calls RemoveObserverAsync, but that finds the observer already
    /// gone and stays silent, so without the notification here GO would keep counting a
    /// watcher the relay has dropped. The periodic batch flush does not heal it — it drains
    /// the dirty sets and returns early when they are empty.
    ///
    /// May run with <c>_sync</c> already held (an enqueue overflowing inside a broadcast);
    /// the lock is re-entrant and the notification only marks a lobby dirty and arms a timer,
    /// so it neither blocks nor calls back into the session.
    /// </summary>
    private void MarkObserverDead(IClientSocket ws)
    {
        bool removed;
        lock (_sync)
        {
            removed = DropObserverLocked(ws);
        }
        if (removed)
            _store.NotifyObserverChange(LobbyId);
    }

    /// <summary>Register a source with its initial health state.</summary>
    public void AddSource(IClientSocket ws, bool isObserver, long bodyLenSeen)
    {
        lock (_sync)
        {
            _sources.Add(ws);
            _sourceObserver[ws] = isObserver;
            _sourceHealth[ws] = new SourceHealth
            {
                Demoted = false,
                LagBytes = 0,
                GapStrikes = 0,
                MismatchStrikes = 0,
                FramesSeen = 0,
                LastFrameAt = TimeSource.Now(),
                BodyLenSeen = bodyLenSeen,
            };
        }
    }

    /// <summary>Whether this source registered as an in-game observer (REGISTER is_observer).</summary>
    public bool IsObserverSource(IClientSocket ws)
    {
        lock (_sync) return _sourceObserver.TryGetValue(ws, out var isObserver) && isObserver;
    }

    /// <summary>Per-source health, exposed for tests (mirrors touching server._source_health).</summary>
    public SourceHealth? GetSourceHealth(IClientSocket ws)
    {
        lock (_sync) return _sourceHealth.GetValueOrDefault(ws);
    }

    public void MarkEndReceived()
    {
        lock (_sync)
        {
            _endReceived = true;
        }
        if (_options.Debug)
            Console.WriteLine($"[LIVESTREAMER] [END] Source sent END for game {LobbyId}");
    }

    /// <summary>
    /// Test-only: mark the header as received without broadcasting or notifying GO
    /// (mirrors test_session_unit.py / test_observer_report.py setting
    /// `session.header_received = True` directly).
    /// </summary>
    internal void SetHeaderReceivedForTest(byte[] header)
    {
        lock (_sync)
        {
            _header.Append(header);
            _headerReceived = true;
        }
    }
    public bool Ended { get { lock (_sync) return _ended; } }
    public bool EndReceived { get { lock (_sync) return _endReceived; } }
    public bool HeaderReceived { get { lock (_sync) return _headerReceived; } }
    public bool HasSources { get { lock (_sync) return _sources.Count > 0; } }
    public bool HasLobby { get { lock (_sync) return _lobby.Count > 0; } }
    public double CreatedAt { get { lock (_sync) return _createdAt; } }
    public double LastActive { get { lock (_sync) return _lastActive; } }
    public int ObserverCount { get { lock (_sync) return _observerWsSet.Count; } }
    public int? LastReportedObservers { get { lock (_sync) return _lastReportedObservers; } set { lock (_sync) _lastReportedObservers = value; } }
    public long? OwnerUserId { get { lock (_sync) return _ownerUserId; } set { lock (_sync) _ownerUserId = value; } }
    public int DelaySeconds { get { lock (_sync) return _delaySeconds; } set { lock (_sync) _delaySeconds = value; } }
    public bool DelayFromGo { get { lock (_sync) return _delayFromGo; } set { lock (_sync) _delayFromGo = value; } }
    public uint LastTickFrame { get { lock (_sync) return _lastTickFrame; } }
    public int BodyLength { get { lock (_sync) return _body.Count; } }
    public Dictionary<string, object?> Lobby { get { lock (_sync) return _lobby; } set { lock (_sync) _lobby = value; } }
    public List<string> LobbyPlayerNames()
    {
        lock (_sync) return RelayStore.LobbyPlayerNames(_lobby);
    }

    public List<IClientSocket> SourcesSnapshot { get { lock (_sync) return [.. _sources]; } }

    // ── Data ingestion (called from the source loop) ────────────────────────

    public async Task ApplyHeaderAsync(IClientSocket ws, byte[] payload)
    {
        if (_sourceDemoted(ws))
            return;
        bool shouldBroadcast;
        lock (_sync)
        {
            if (!_headerReceived)
            {
                _header.Append(payload);
                _headerReceived = true;
                _lastActive = TimeSource.Now();
                shouldBroadcast = true;
                if (_options.Debug)
                    Console.WriteLine($"[LIVESTREAMER] [HEADER] Game {LobbyId}: stored header ({payload.Length} bytes)");
            }
            else
            {
                shouldBroadcast = false;
                if (!_header.SliceEquals(0, payload) || _header.Count != payload.Length)
                    Console.WriteLine($"[LIVESTREAMER] [WARN] HEADER mismatch from another source for game {LobbyId}: " +
                                      $"stored={_header.Count}B, received={payload.Length}B");
            }
        }
        if (!shouldBroadcast)
            return;
        BroadcastEnvelope(MsgHeader, payload);
        // The header is what makes a session watchable — before it arrives an observer would
        // connect and sit staring at nothing. Receiving it is therefore the moment the stream
        // becomes live as far as GO is concerned, and this report is what puts the lobby into
        // GO's livestream menu. It goes out now rather than through the batch.
        await _store.NotifyStreamLive(LobbyId);
    }

    public async Task ApplyPatchAsync(IClientSocket ws, byte[] payload)
    {
        if (_sourceDemoted(ws))
            return;
        if (payload.Length < 8)
        {
            Console.WriteLine($"[LIVESTREAMER] [WARN] PATCH payload too short: {payload.Length} bytes");
            return;
        }
        int offset = checked((int)BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(0, 4)));
        int patchLen = checked((int)BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(4, 4)));
        byte[] patchData = payload.AsSpan(8, Math.Min(patchLen, payload.Length - 8)).ToArray();

        lock (_sync)
        {
            _header.Patch(offset, patchData);
            _lastActive = TimeSource.Now();
            if (_options.Debug)
                Console.WriteLine($"[LIVESTREAMER] [PATCH] Game {LobbyId}: offset={offset} len={patchLen} header_size={_header.Count}");
        }
        BroadcastEnvelope(MsgPatch, payload);
    }

    public async Task ApplyBodyAsync(IClientSocket ws, byte[] payload)
    {
        if (_sourceDemoted(ws))
            return;
        if (payload.Length < 8)
        {
            Console.WriteLine($"[LIVESTREAMER] [WARN] BODY payload too short: {payload.Length} bytes");
            return;
        }
        long offset = (long)BinaryPrimitives.ReadUInt64LittleEndian(payload.AsSpan(0, 8));
        byte[] data = payload.AsSpan(8).ToArray();

        bool shouldBroadcast = false;
        List<IClientSocket> targets = [];
        lock (_sync)
        {
            long bodyLen = _body.Count;

            if (offset == bodyLen)
            {
                _body.Append(data);
                _recordBodyHistory(TimeSource.Now(), _body.Count);
                _lastActive = TimeSource.Now();
                shouldBroadcast = true;
                _recordFrameHealth(ws, lagBytes: 0);
                // Fix the recipient list here, while still holding the lock that guards the
                // append. An observer registering after this point records a catch-up limit
                // that already covers these bytes, so sending them the live chunk as well
                // would duplicate it.
                targets = [.. _observerWsSet];
                if (_body.Count < 5000 || _body.Count % 50000 == 0)
                    Console.WriteLine($"[LIVESTREAMER] [BODY] Game {LobbyId}: +{data.Length}B @ offset={offset} total={_body.Count}");
            }
            else if (offset < bodyLen)
            {
                int overlap = (int)Math.Min(data.Length, bodyLen - offset);
                bool match = _body.SliceEquals((int)offset, data.AsSpan(0, overlap));
                if (!match)
                {
                    Console.WriteLine($"[LIVESTREAMER] [WARN] BODY desync for game {LobbyId}: " +
                                      $"offset={offset} overlap={overlap} mismatch!");
                    _recordFrameHealth(ws, lagBytes: bodyLen - offset, mismatch: true);
                }
                else
                {
                    _recordFrameHealth(ws, lagBytes: bodyLen - offset);
                }
            }
            else
            {
                Console.WriteLine($"[LIVESTREAMER] [ERROR] BODY gap for game {LobbyId}: " +
                                  $"offset={offset} > body_len={bodyLen} — dropping, investigate source");
                _recordFrameHealth(ws, lagBytes: offset - bodyLen, gap: true);
            }
        }

        if (!shouldBroadcast)
            return;
        long fileOffset = HeaderLength + offset;
        byte[] framed = new byte[8 + data.Length];
        BinaryPrimitives.WriteUInt64LittleEndian(framed, (ulong)fileOffset);
        data.CopyTo(framed.AsSpan(8));
        BroadcastEnvelope(MsgBody, framed, targets);
        // Delay hold: held observers' bytes become available at arrival + delay, so every
        // append is also a chance to advance the shared delayed edge.
        FlushHeldObservers();
    }

    public async Task ApplyTickAsync(IClientSocket ws, byte[] payload)
    {
        if (_sourceDemoted(ws))
            return;
        if (payload.Length < 4)
        {
            Console.WriteLine($"[LIVESTREAMER] [WARN] TICK payload too short: {payload.Length} bytes");
            return;
        }
        uint frame = BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(0, 4));

        List<IClientSocket> targets;
        lock (_sync)
        {
            // Monotonic. All-push means several sources forward the same stream, and a source
            // running behind would otherwise pull the advertised edge back — an observer that
            // already simulated to the higher frame cannot un-simulate it.
            if (frame <= _lastTickFrame)
                return;
            _lastTickFrame = frame;
            _recordTickHistory(TimeSource.Now(), frame);
            targets = [.. _observerWsSet];
        }
        BroadcastEnvelope(MsgTick, payload, targets);
        // Held observers never see this live tick (see BroadcastEnvelope) - they get a delayed
        // one, bound by the same watermark as body bytes, from the flush path below.
        FlushHeldObservers();
    }

    /// <summary>
    /// The source's logic frame rate and ping, [logicFps u32 LE][pingMs u32 LE]. Display-only
    /// telemetry: nothing here is simulated from, so unlike the tick there is no ordering
    /// requirement against the body, and no monotonic rule - the values legitimately go both up
    /// and down. Stored as the latest known pair, forwarded live to unheld observers and released
    /// to held ones on the delayed boundary.
    /// </summary>
    public async Task ApplyStatsAsync(IClientSocket ws, byte[] payload)
    {
        if (_sourceDemoted(ws))
            return;
        if (payload.Length < 8)
        {
            Console.WriteLine($"[LIVESTREAMER] [WARN] STATS payload too short: {payload.Length} bytes");
            return;
        }

        List<IClientSocket> targets;
        lock (_sync)
        {
            _lastStats = payload[..8];
            _recordStatsHistory(TimeSource.Now(), _lastStats);
            targets = [.. _observerWsSet];
        }
        BroadcastEnvelope(MsgStats, payload, targets);
        FlushHeldObservers();
        await Task.CompletedTask;
    }

    public void TouchSource(IClientSocket ws)
    {
        lock (_sync)
        {
            if (_sourceHealth.TryGetValue(ws, out var health) && !health.Demoted)
            {
                health.LastFrameAt = TimeSource.Now();
                health.BodyLenSeen = _body.Count;
            }
        }
    }

    private void _recordFrameHealth(IClientSocket ws, long lagBytes = 0, bool gap = false, bool mismatch = false)
    {
        lock (_sync)
        {
            if (!_sourceHealth.TryGetValue(ws, out var health) || health.Demoted)
                return;
            health.LagBytes += Math.Max(0, lagBytes);
            health.FramesSeen++;
            if (gap)
                health.GapStrikes++;
            if (mismatch)
                health.MismatchStrikes++;
        }
    }

    public void SaveReplay()
    {
        byte[] header;
        byte[] body;
        lock (_sync)
        {
            header = _header.ToArray();
            body = _body.ToArray();
        }
        if (header.Length == 0)
            return;
        Directory.CreateDirectory("replays");
        string filename = $"replays/{LobbyId}.rep";
        using var stream = File.Create(filename);
        stream.Write(header);
        stream.Write(body);
        if (_options.Debug)
            Console.WriteLine($"[LIVESTREAMER] [SAVE] Wrote {filename} ({header.Length}+{body.Length} bytes)");
    }

    // ── Source lifecycle ────────────────────────────────────────────────────

    /// <summary>Called when a source disconnects. Ends the session if all sources are gone.</summary>
    public async Task RemoveSourceAsync(IClientSocket ws)
    {
        bool shouldBroadcastEnd = false;
        bool shouldSave = false;
        bool endedHere = false;
        lock (_sync)
        {
            _sources.Remove(ws);
            _sourceHealth.Remove(ws);
            _sourceObserver.Remove(ws);
            _spectatorRate.Remove(ws);
            if (_sources.Count == 0 && _endReceived)
            {
                _ended = true;
                shouldBroadcastEnd = true;
                shouldSave = true;
                endedHere = true;
                if (_options.Debug)
                    Console.WriteLine($"[LIVESTREAMER] [END] Game {LobbyId}: all sources gone, END was received");
            }
            else if (_sources.Count == 0)
            {
                _ended = true;
                shouldSave = true;
                endedHere = true;
                if (_options.Debug)
                    Console.WriteLine($"[LIVESTREAMER] [SOURCE_GONE] Game {LobbyId}: last source disconnected");
            }
            else if (_options.Debug)
            {
                Console.WriteLine($"[LIVESTREAMER] [SOURCE_GONE] source disconnected from game {LobbyId}... " +
                                  $"({_sources.Count} remaining)");
            }
        }
        if (shouldSave)
            SaveReplay();
        if (shouldBroadcastEnd)
        {
            SaveReplay();
            BroadcastEnvelope(MsgEnd, []);
        }
        else if (endedHere)
        {
            // Last source gone WITHOUT an END frame: the streamer died or its connection
            // dropped mid-game. Tell any observers so they can show "stream lost".
            if (ObserverCount > 0)
            {
                try
                {
                    string reasonJson = JsonSerializer.Serialize(new { reason = "sources-gone",
                        msg = "all streamers disconnected" }, JsonOpts);
                    BroadcastEnvelope(MsgError, Encoding.UTF8.GetBytes(reasonJson));
                }
                catch (Exception)
                {
                    // ignore — shutdown must not fail
                }
            }
        }
        if (endedHere)
        {
            _store.NotifyStreamEnded(LobbyId);
        }
        else if (!Ended)
        {
            // A source left but the session lives on. If it was the last active (non-demoted)
            // pusher, re-promote the least-bad demoted backup so the stream continues.
            await MaybePromoteBackupAsync();
        }
    }

    // Shared JSON options: verbatim property names (the wire contract spells its keys exactly).
    internal static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = null,
    };
}
