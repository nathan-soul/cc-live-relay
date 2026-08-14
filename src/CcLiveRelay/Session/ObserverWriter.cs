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

    public ObserverWriter(IClientSocket ws, int queueFrames, Action<IClientSocket> onDead)
    {
        _ws = ws;
        _onDead = onDead;
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
        try
        {
            await foreach (var frame in _channel.Reader.ReadAllAsync())
            {
                if (!_ws.IsOpen)
                {
                    _onDead(_ws);
                    return;
                }
                await _ws.SendAsync(frame);
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
