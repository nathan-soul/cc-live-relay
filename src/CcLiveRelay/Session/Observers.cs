using System.Text;
using System.Text.Json;
using System.Buffers.Binary;
using CcLiveRelay.Protocol;
using CcLiveRelay.Util;

using static CcLiveRelay.Protocol.BinaryEnvelope;
namespace CcLiveRelay.Session;

public sealed partial class GameSession
{
    // ── Observer lifecycle ──────────────────────────────────────────────────

    public int HeaderLength { get { lock (_sync) return _header.Count; } }

    /// <summary>
    /// Register an observer and enqueue its catch-up in one atomic step.
    ///
    /// Registration and catch-up share one <c>_sync</c> hold: broadcasts serialize on the
    /// same lock, so no live chunk can be delivered ahead of the catch-up chunks that precede
    /// it. Observers write each chunk at its absolute file offset, so an interleaving live
    /// chunk would leave a hole in the observer's file.
    ///
    /// priority marks a privileged watcher (admin / user_priority = Viewer, stamped on the
    /// watch ticket by GO): it bypasses the delay hold and watches the live edge. Everyone
    /// else on a delayed stream is held.
    /// </summary>
    public bool AddObserver(IClientSocket ws, bool priority, long lastOffset = 0)
    {
        lock (_sync)
        {
            if (_observerWsSet.Count >= _options.MaxObserversPerGame)
                return false;

            _observerWriters[ws] = new ObserverWriter(ws, _options.ObserverQueueFrames, MarkObserverDead, _options.Debug);

            // Held observers start at the delayed edge: their catch-up covers only bytes
            // older than the delay, and the pointer doubles as "delivered so far".
            bool held = !priority && _delaySeconds > 0;
            _observerHeld[ws] = held;
            _observerCatchupLimit[ws] = held
                ? Math.Min(_body.Count, DelayedWatermark(TimeSource.Now()))
                : _body.Count;
            _observerWsSet.Add(ws);
            _lastActive = TimeSource.Now();

            // Catch-up goes into the writer queue before the observer becomes visible to
            // broadcasts — enqueues are serialized under _sync, so order is exact.
            SendCatchupLocked(ws, lastOffset);
        }
        _store.NotifyObserverChange(LobbyId);
        return true;
    }

    public Task RemoveObserverAsync(IClientSocket ws)
    {
        bool removed;
        lock (_sync) removed = DropObserverLocked(ws);
        if (removed)
            _store.NotifyObserverChange(LobbyId);
        return Task.CompletedTask;
    }

    /// <summary>Caller must hold <c>_sync</c>. Idempotent.</summary>
    private bool DropObserverLocked(IClientSocket ws)
    {
        if (!_observerWsSet.Remove(ws))
            return false;
        if (_observerWriters.Remove(ws, out var writer))
            writer.Stop();
        _observerCatchupLimit.Remove(ws);
        _observerHeld.Remove(ws);
        _observerLastFlushAt.Remove(ws);
        _observerLastSentTick.Remove(ws);
        _spectatorRate.Remove(ws);
        // Wake the watch loop (blocked in ReceiveAsync) so the endpoint winds down.
        _ = Task.Run(async () =>
        {
            try { await ws.CloseAsync(); }
            catch (Exception) { /* already gone */ }
        });
        return true;
    }

    private bool IsHeld(IClientSocket ws)
    {
        lock (_sync) return _observerHeld.GetValueOrDefault(ws, false);
    }

    /// <summary>
    /// Enqueue config + header + body[lastOffset:limit] + tick + end + chat history for one
    /// observer. Caller must hold <c>_sync</c>. All frames land in the observer's writer
    /// queue in order; nothing here touches the socket.
    /// </summary>
    private void SendCatchupLocked(IClientSocket ws, long lastOffset)
    {
        byte[] headerSnapshot;
        bool endedSnapshot;
        bool held;
        uint tickSnapshot;
        int delaySnapshot;
        long limit;
        byte[] bodySnapshot;
        double catchupNow = TimeSource.Now();
        lock (_sync)
        {
            headerSnapshot = _header.ToArray();
            endedSnapshot = _ended;
            held = _observerHeld.GetValueOrDefault(ws, false);
            // A held observer must never see the raw live tick (that is the true, undelayed
            // frame - exactly what the byte hold exists to withhold); it gets whatever tick was
            // current as of now-delay instead, same boundary as its body catch-up slice below.
            tickSnapshot = held ? DelayedTickFrame(catchupNow) : _lastTickFrame;
            if (held)
                _observerLastSentTick[ws] = tickSnapshot;
            // Held observers get delay_seconds: 0 — the relay's byte-level hold IS the delay,
            // and the client must not double-hold on top of it.
            delaySnapshot = held ? 0 : _delaySeconds;
            // Stop exactly where the delivered edge begins. Snapshotting the whole body
            // instead would resend anything appended between registration and now.
            limit = Math.Min(_body.Count, _observerCatchupLimit.GetValueOrDefault(ws, _body.Count));
            bodySnapshot = _body.Slice(0, (int)limit);
        }

        // Must precede the HEADER: receiving the header is what starts playback on the
        // observer, and the pre-roll buffer latches against the delay.
        var configJson = JsonSerializer.Serialize(new
        {
            role = "observer",
            lobbyid = LobbyId,
            delay_seconds = delaySnapshot,
        }, JsonOpts);
        if (!EnqueueObserverFrame(ws, BinaryEnvelope.Pack(MsgRole, Encoding.UTF8.GetBytes(configJson))))
            return;

        if (headerSnapshot.Length > 0)
        {
            if (!EnqueueObserverFrame(ws, BinaryEnvelope.Pack(MsgHeader, headerSnapshot)))
                return;
        }

        long startOffset = Math.Min(lastOffset, limit);
        long sliceLen = limit - startOffset;
        int headerSize = headerSnapshot.Length;
        for (long chunkOff = 0; chunkOff < sliceLen; chunkOff += _options.ChunkSize)
        {
            int chunkLen = (int)Math.Min(_options.ChunkSize, sliceLen - chunkOff);
            byte[] chunk = bodySnapshot.AsSpan((int)(startOffset + chunkOff), chunkLen).ToArray();
            byte[] chunkPayload = new byte[8 + chunkLen];
            BinaryPrimitives.WriteUInt64LittleEndian(chunkPayload,
                (ulong)(headerSize + startOffset + chunkOff));
            chunk.CopyTo(chunkPayload.AsSpan(8));
            if (!EnqueueObserverFrame(ws, BinaryEnvelope.Pack(MsgBody, chunkPayload)))
                return;
        }

        // Frame heartbeat for the joining observer, after the body it belongs to. Ticks are
        // broadcast, not stored, so without this a joiner would sit on the record-derived edge
        // until the next one arrives - held observers included, now that tickSnapshot above is
        // already the delayed value for them rather than the raw live one.
        if (tickSnapshot != 0)
        {
            if (!EnqueueObserverFrame(ws, BinaryEnvelope.Pack(MsgTick, BinaryEnvelope.PackU32(tickSnapshot))))
                return;
        }

        if (endedSnapshot)
        {
            if (held)
            {
                // Stream ended while this observer was joining: nothing is left to spoil,
                // so drain the rest of its body now, then the END frame.
                EnqueueHeldFlushLocked(ws, force: true);
            }
            if (!EnqueueObserverFrame(ws, BinaryEnvelope.Pack(MsgEnd)))
                return;
        }

        // Player-chat history slice for the joining observer, sent after the body. Order is
        // irrelevant — the observer frame-gates them.
        foreach (var chatPayload in ChatCatchupSlice())
        {
            if (!EnqueueObserverFrame(ws, BinaryEnvelope.Pack(MsgChat, chatPayload)))
                return;
        }

        if (_options.Debug)
            Console.WriteLine($"[OBSERVER] [CATCHUP] Queued header ({headerSnapshot.Length}B) + body ({bodySnapshot.Length}B, offset={lastOffset}) to observer");
    }

    // ── Delay hold ──
    //
    // A held observer never receives BODY bytes directly: the session owns a single delayed
    // edge (the arrival history + watermark), and each held observer only tracks how far it
    // has received (_observerCatchupLimit as a pointer). Delivering the same held copy to
    // every watcher — rather than buffering per observer — is the in-process equivalent of
    // the future dispatcher tier's worker B.

    /// <summary>Body length as of `now - delay`: the newest byte a held observer may receive.
    /// Binary search over the per-append arrival history.</summary>
    public long DelayedWatermark(double now)
    {
        lock (_sync)
        {
            if (_delaySeconds <= 0)
                return _body.Count;
            double cutoff = now - _delaySeconds;
            var hist = _bodyHistory;
            if (hist.Count == 0 || hist[0].Timestamp > cutoff)
                return 0;
            if (hist[^1].Timestamp <= cutoff)
                return hist[^1].BodyLen;
            int lo = 0, hi = hist.Count - 1;
            while (lo < hi)
            {
                int mid = (lo + hi + 1) / 2;
                if (hist[mid].Timestamp <= cutoff)
                    lo = mid;
                else
                    hi = mid - 1;
            }
            return hist[lo].BodyLen;
        }
    }

    private readonly struct BodyHistoryEntry(double timestamp, long bodyLen)
    {
        public readonly double Timestamp = timestamp;
        public readonly long BodyLen = bodyLen;
    }

    /// <summary>Recorded (arrival time, body length) pairs for the watermark lookup.</summary>
    private readonly List<BodyHistoryEntry> _bodyHistory = [];

    /// <summary>Debug only: wall-clock of each held observer's last delivered flush, to log the
    /// gap between deliveries while chasing the 2026-08-15 "cuts out every second" report.</summary>
    private readonly Dictionary<IClientSocket, double> _observerLastFlushAt = [];

    /// <summary>Highest delayed-tick frame already sent to each held observer, so the flush
    /// only enqueues a MSG_TICK when the delayed value actually advances.</summary>
    private readonly Dictionary<IClientSocket, uint> _observerLastSentTick = [];

    private readonly struct TickHistoryEntry(double timestamp, uint frame)
    {
        public readonly double Timestamp = timestamp;
        public readonly uint Frame = frame;
    }

    /// <summary>
    /// Arrival-time history of the streamer's frame heartbeat (MSG_TICK), mirroring
    /// _bodyHistory. A held observer never sees the raw heartbeat (that would tell it the true,
    /// undelayed live frame - the exact thing the byte-level hold exists to withhold); instead
    /// DelayedTickFrame looks up whatever tick was current as of now-delay, the same boundary
    /// DelayedWatermark applies to bytes. Without this, a held observer's only source of "what
    /// frame is the game at" is the record edge itself, which - per getLiveEdge()'s own comment
    /// client-side - sawtooths in ~1.7s jumps on any stream that is not command-dense every
    /// frame: this is the fix for the 2026-08-15 "cuts out every second, falls further behind"
    /// report, which turned out to be a client-side gate freezing during exactly those gaps.
    /// </summary>
    private readonly List<TickHistoryEntry> _tickHistory = [];

    private void _recordTickHistory(double ts, uint frame)
    {
        _tickHistory.Add(new TickHistoryEntry(ts, frame));
        double cutoff = ts - 2 * _options.MaxDelaySeconds;
        int trim = 0;
        while (trim < _tickHistory.Count - 1 && _tickHistory[trim].Timestamp < cutoff)
            trim++;
        if (trim > 0)
            _tickHistory.RemoveRange(0, trim);
        while (_tickHistory.Count > _options.BodyHistoryMax)
            _tickHistory.RemoveAt(0);
    }

    /// <summary>The heartbeat frame as of `now - delay`: the same lookup as DelayedWatermark,
    /// against tick arrivals instead of body length. 0 means no tick has qualified yet (the
    /// caller must treat that as "say nothing", matching the live-tick convention).</summary>
    public uint DelayedTickFrame(double now)
    {
        lock (_sync)
        {
            if (_delaySeconds <= 0)
                return _lastTickFrame;
            double cutoff = now - _delaySeconds;
            var hist = _tickHistory;
            if (hist.Count == 0 || hist[0].Timestamp > cutoff)
                return 0;
            if (hist[^1].Timestamp <= cutoff)
                return hist[^1].Frame;
            int lo = 0, hi = hist.Count - 1;
            while (lo < hi)
            {
                int mid = (lo + hi + 1) / 2;
                if (hist[mid].Timestamp <= cutoff)
                    lo = mid;
                else
                    hi = mid - 1;
            }
            return hist[lo].Frame;
        }
    }

    /// <summary>
    /// Record one (arrival time, body length) pair. Timestamps are non-decreasing (appends
    /// are sequential), so the list doubles as a sorted timeline. Trimmed to a 2x-max-delay
    /// window plus a hard entry cap, so it stays small even for a long match.
    /// </summary>
    private void _recordBodyHistory(double ts, long bodyLen)
    {
        _bodyHistory.Add(new BodyHistoryEntry(ts, bodyLen));
        double cutoff = ts - 2 * _options.MaxDelaySeconds;
        int trim = 0;
        while (trim < _bodyHistory.Count - 1 && _bodyHistory[trim].Timestamp < cutoff)
            trim++;
        if (trim > 0)
            _bodyHistory.RemoveRange(0, trim);
        while (_bodyHistory.Count > _options.BodyHistoryMax)
            _bodyHistory.RemoveAt(0);
    }

    /// <summary>BODY frames covering body[start:end], chunked, with absolute file offsets.</summary>
    private List<byte[]> PackBodyFrames(long start, long end)
    {
        var frames = new List<byte[]>();
        int headerSize;
        lock (_sync) headerSize = _header.Count;
        for (long chunkOff = start; chunkOff < end; chunkOff += _options.ChunkSize)
        {
            long chunkEnd = Math.Min(chunkOff + _options.ChunkSize, end);
            byte[] chunk;
            lock (_sync) chunk = _body.Slice((int)chunkOff, (int)(chunkEnd - chunkOff));
            byte[] payload = new byte[8 + chunk.Length];
            BinaryPrimitives.WriteUInt64LittleEndian(payload, (ulong)(headerSize + chunkOff));
            chunk.CopyTo(payload.AsSpan(8));
            frames.Add(BinaryEnvelope.Pack(MsgBody, payload));
        }
        return frames;
    }

    /// <summary>
    /// Advance one held observer's delivered edge to the current watermark and enqueue the
    /// chunks, then do the same for its delayed tick heartbeat. force=True ignores both
    /// watermarks and delivers everything left (stream end: nothing is left to spoil). The tick
    /// check runs independently of whether body advanced - a heartbeat arrives on a fixed
    /// cadence regardless of command activity, which is the entire reason it exists (see
    /// DelayedTickFrame): gating it behind "body also moved" would silently reintroduce the
    /// sawtooth this is meant to fix. Caller must hold <c>_sync</c>; nothing here touches the
    /// socket.
    /// </summary>
    private void EnqueueHeldFlushLocked(IClientSocket ws, double? now = null, bool force = false)
    {
        if (!_observerHeld.GetValueOrDefault(ws, false))
            return;
        double effectiveNow = now ?? TimeSource.Now();

        long limit = force ? _body.Count : DelayedWatermark(effectiveNow);
        long pointer = _observerCatchupLimit.GetValueOrDefault(ws, 0);
        bool bodyAdvanced = limit > pointer;
        if (bodyAdvanced)
        {
            foreach (var frame in PackBodyFrames(pointer, limit))
            {
                if (!EnqueueObserverFrame(ws, frame))
                    return;   // queue full -> observer dropped; nothing further to deliver
            }
            _observerCatchupLimit[ws] = limit;

            if (_options.Debug)
            {
                double gapMs = _observerLastFlushAt.TryGetValue(ws, out var lastAt)
                    ? (effectiveNow - lastAt) * 1000.0
                    : -1;
                _observerLastFlushAt[ws] = effectiveNow;
                Console.WriteLine($"[OBSERVER] [FLUSH] {DateTime.Now:HH:mm:ss.fff} game={LobbyId} " +
                    $"ws={ws.GetHashCode()} delta={limit - pointer}B gapMs={gapMs:F0} pointer={pointer} watermark={limit}");
            }
        }

        uint tickFrame = force ? _lastTickFrame : DelayedTickFrame(effectiveNow);
        uint lastSentTick = _observerLastSentTick.GetValueOrDefault(ws, 0u);
        if (tickFrame > lastSentTick)
        {
            if (EnqueueObserverFrame(ws, BinaryEnvelope.Pack(MsgTick, BinaryEnvelope.PackU32(tickFrame))))
                _observerLastSentTick[ws] = tickFrame;
        }
    }

    /// <summary>
    /// Advance one held observer's delivered edge to the current watermark (flush-on-append).
    /// Cheap when idle: only the pointer math + queue pushes, no socket I/O.
    /// </summary>
    public void FlushHeldObserver(IClientSocket ws, double? now = null, bool force = false)
    {
        lock (_sync) EnqueueHeldFlushLocked(ws, now, force);
    }

    /// <summary>
    /// Advance every held observer to the shared delayed edge (flush-on-append). All held
    /// observers share the same watermark, so the edge is computed once per observer. Cheap
    /// when idle. The global ticker calls this too, to catch chunks whose delay elapsed while
    /// no append happened nearby.
    /// </summary>
    public void FlushHeldObservers(double? now = null)
    {
        List<IClientSocket> held;
        lock (_sync)
        {
            if (_observerHeld.Count == 0)
                return;
            held = [.. _observerWsSet];
        }
        double effectiveNow = now ?? TimeSource.Now();
        foreach (var ws in held)
        {
            FlushHeldObserver(ws, effectiveNow, force: false);
        }
    }
}
