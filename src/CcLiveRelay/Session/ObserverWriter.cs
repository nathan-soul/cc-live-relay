using System.Diagnostics;
using System.Threading.Channels;

namespace CcLiveRelay.Session;

/// <summary>
/// Per-observer send queue. A single writer task owns the socket's write side, so frames are
/// delivered exactly in enqueue order and a slow observer only backs up its own channel — it
/// can never gate the source receive loop.
///
/// This replaces the per-observer SemaphoreSlim + inline awaited sends. Under the old model
/// every BODY message made the source loop await the fan-out to all observers (and the
/// delayed-edge flush) before reading the next message; with several streamers on one relay
/// that backpressured every source and turned slow observers into game-wide stalls.
///
/// All enqueues happen under the session's _sync lock (see GameSession.EnqueueObserverFrame),
/// which makes per-observer order exact: catch-up, held-flush chunks and live frames cannot
/// interleave.
/// </summary>
public sealed class ObserverWriter
{
    private readonly IClientSocket _ws;
    private readonly Channel<byte[]> _channel;
    private readonly Action<IClientSocket> _onDead;
    private readonly Task _drainTask;
    private volatile bool _stopped;
    private readonly bool _debug;

    public ObserverWriter(IClientSocket ws, int queueFrames, Action<IClientSocket> onDead, bool debug = false)
    {
        _ws = ws;
        _onDead = onDead;
        _debug = debug;
        _channel = Channel.CreateBounded<byte[]>(new BoundedChannelOptions(queueFrames)
        {
            // Wait is the only mode whose TryWrite reports a full queue (it returns false);
            // the Drop* modes accept the write and discard a frame, which would silently punch
            // a hole in the observer's file and leave it connected and stalled forever. Nothing
            // here ever blocks on that "wait": the queue is only ever fed by TryEnqueue's
            // TryWrite, never by WriteAsync.
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
        });
        _drainTask = Task.Run(DrainAsync);
    }

    /// <summary>
    /// Queue one frame for delivery. False = queue full: the observer cannot keep up, so it is
    /// dropped (a rejoin re-serves catch-up from the body). Caller must hold the session lock.
    /// </summary>
    public bool TryEnqueue(byte[] frame)
    {
        if (_stopped)
            return false;
        if (_channel.Writer.TryWrite(frame))
            return true;
        _onDead(_ws);
        return false;
    }

    private async Task DrainAsync()
    {
        // Debug-only instrumentation for the 2026-08-15 "held observer falls further and
        // further behind" report: is the per-frame SendAsync itself the bottleneck (queue
        // backs up because the drain task can't keep pace), or is data simply not being
        // flushed often enough upstream (see Observers.cs [OBSERVER][FLUSH])? Logged at most
        // once/second so a 60fps stream doesn't spam the console.
        var sw = _debug ? Stopwatch.StartNew() : null;
        long sendCount = 0;
        long sendMsTotal = 0;
        long maxQueueDepth = 0;
        double lastLogAt = _debug ? Util.TimeSource.Now() : 0;
        try
        {
            await foreach (var frame in _channel.Reader.ReadAllAsync())
            {
                if (!_ws.IsOpen)
                {
                    _onDead(_ws);
                    return;
                }
                if (_debug)
                {
                    long depth = _channel.Reader.Count;
                    if (depth > maxQueueDepth)
                        maxQueueDepth = depth;
                    sw!.Restart();
                }
                await _ws.SendAsync(frame);
                if (_debug)
                {
                    sendCount++;
                    sendMsTotal += sw!.ElapsedMilliseconds;
                    double now = Util.TimeSource.Now();
                    if (now - lastLogAt >= 1.0)
                    {
                        Console.WriteLine($"[OBSERVER] [SEND] {DateTime.Now:HH:mm:ss.fff} ws={_ws.GetHashCode()} " +
                            $"sends={sendCount} avgMs={(sendCount > 0 ? sendMsTotal / (double)sendCount : 0):F1} " +
                            $"maxQueueDepth={maxQueueDepth}");
                        sendCount = 0;
                        sendMsTotal = 0;
                        maxQueueDepth = 0;
                        lastLogAt = now;
                    }
                }
            }
        }
        catch (Exception e)
        {
            Console.WriteLine($"[OBSERVER] [WARN] send to observer failed " +
                              $"({e.GetType().Name}: {e.Message}), marking dead");
            _onDead(_ws);
        }
    }

    /// <summary>Stop delivering (observer removed). Pending frames are dropped.</summary>
    public void Stop()
    {
        _stopped = true;
        _channel.Writer.TryComplete();
    }
}
