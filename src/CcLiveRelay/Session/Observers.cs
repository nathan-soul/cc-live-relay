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

            _observerWriters[ws] = new ObserverWriter(ws, _options.ObserverQueueFrames, MarkObserverDead);

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
        lock (_sync)
        {
            headerSnapshot = _header.ToArray();
            endedSnapshot = _ended;
            held = _observerHeld.GetValueOrDefault(ws, false);
            tickSnapshot = _lastTickFrame;
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
        // broadcast, not stored, so without this a joiner would sit on the record-derived
        // edge until the next one arrives. Held observers are excluded for the same reason
        // they are excluded from live ticks.
        if (tickSnapshot != 0 && !held)
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
    /// chunks. force=True ignores the watermark and delivers everything left (stream end:
    /// nothing is left to spoil). Caller must hold <c>_sync</c>; nothing here touches the
    /// socket.
    /// </summary>
    private void EnqueueHeldFlushLocked(IClientSocket ws, double? now = null, bool force = false)
    {
        if (!_observerHeld.GetValueOrDefault(ws, false))
            return;
        double effectiveNow = now ?? TimeSource.Now();
        long limit = force ? _body.Count : DelayedWatermark(effectiveNow);
        long pointer = _observerCatchupLimit.GetValueOrDefault(ws, 0);
        if (limit <= pointer)
            return;
        foreach (var frame in PackBodyFrames(pointer, limit))
        {
            if (!EnqueueObserverFrame(ws, frame))
                return;   // queue full -> observer dropped; nothing further to deliver
        }
        _observerCatchupLimit[ws] = limit;
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
