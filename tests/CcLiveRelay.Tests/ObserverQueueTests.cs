using System.Buffers.Binary;
using System.Diagnostics;
using CcLiveRelay.Config;
using CcLiveRelay.Protocol;
using CcLiveRelay.Services;
using CcLiveRelay.Session;
using CcLiveRelay.State;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace CcLiveRelay.Tests;

/// <summary>
/// Tests for the per-observer send queue (see ObserverWriter). Backpressure is not removed by
/// that design, it is relocated: a watcher that cannot keep up fills its own bounded queue and
/// is dropped, instead of making the source loop wait on its socket. The three properties that
/// buys us are exercised here — the source is never gated, the overflow drop actually happens
/// and closes the socket, and a rejoin is re-served from the body — plus the reporting the drop
/// owes GO.
/// </summary>
public class ObserverQueueTests
{
    private const int HeaderLen = 16;
    private const int ChunkLen = 10;

    /// <summary>Observer socket whose sends block until released — the "slow watcher".</summary>
    private sealed class BlockingSocket : IClientSocket
    {
        private readonly TaskCompletionSource _gate =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _sent;

        public bool Closed { get; private set; }
        public int Sent => Volatile.Read(ref _sent);
        public bool IsOpen => !Closed;

        public async Task SendAsync(byte[] frame)
        {
            await _gate.Task;
            Interlocked.Increment(ref _sent);
        }

        public Task CloseAsync()
        {
            Closed = true;
            return Task.CompletedTask;
        }

        public void Release() => _gate.TrySetResult();
    }

    private static byte[] BodyPayload(long offset, int length, byte fill)
    {
        var payload = new byte[8 + length];
        BinaryPrimitives.WriteUInt64LittleEndian(payload, (ulong)offset);
        payload.AsSpan(8).Fill(fill);
        return payload;
    }

    private static async Task<GameSession> SeededSessionAsync(RelayOptions options, RelayStore store,
                                                              string lobbyId, FakeSocket source)
    {
        var session = new GameSession(lobbyId, options, store);
        await session.ApplyHeaderAsync(source, new byte[HeaderLen]);
        return session;
    }

    /// <summary>Append `count` chunks to the body, in sequence from the current end.</summary>
    private static async Task AppendAsync(GameSession session, FakeSocket source, int count)
    {
        for (int i = 0; i < count; i++)
            await session.ApplyBodyAsync(source, BodyPayload(session.BodyLength, ChunkLen, (byte)'B'));
    }

    private static async Task WaitForAsync(Func<bool> condition, string what, int timeoutMs = 5000)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new Xunit.Sdk.XunitException($"timed out waiting for {what}");
            await Task.Delay(20);
        }
    }

    /// <summary>File-offset spans of the BODY frames a socket received, in delivery order.</summary>
    private static void CheckExactlyOnce(string label, FakeSocket ws, int totalBody)
    {
        long cursor = HeaderLen;
        var spans = new List<(long Start, long End)>();
        foreach (var (type, payload) in ws.Frames)
        {
            if (type != BinaryEnvelope.MsgBody)
                continue;
            long start = (long)BinaryPrimitives.ReadUInt64LittleEndian(payload.AsSpan(0, 8));
            long end = start + payload.Length - 8;
            spans.Add((start, end));
            if (start != cursor)
                throw new Xunit.Sdk.XunitException(
                    $"{label}: {(start < cursor ? "overlap" : "gap")}: chunk at {start}, expected {cursor} " +
                    $"(spans={string.Join(";", spans)})");
            cursor = end;
        }
        Assert.True(cursor == HeaderLen + totalBody,
            $"{label}: covered to {cursor}, expected {HeaderLen + totalBody} (spans={string.Join(";", spans)})");
    }

    [Fact]
    public async Task StuckObserver_DoesNotGateTheSource()
    {
        var options = new RelayOptions();          // queue big enough that nothing is dropped
        var store = new RelayStore(options);
        var source = new FakeSocket();
        var session = await SeededSessionAsync(options, store, "queue_nogate", source);

        var slow = new BlockingSocket();
        Assert.True(session.AddObserver(slow, priority: true));

        // Under the old awaited fan-out this loop could not finish: the first BODY broadcast
        // waited on the stuck socket. Time it out explicitly so a regression fails the test
        // instead of hanging the run.
        var stopwatch = Stopwatch.StartNew();
        var pump = Task.Run(() => AppendAsync(session, source, 200));
        var finished = await Task.WhenAny(pump, Task.Delay(5000));
        stopwatch.Stop();

        Assert.True(finished == pump,
            $"source loop was gated by a stuck observer (still running after {stopwatch.ElapsedMilliseconds} ms)");
        await pump;
        Assert.Equal(0, slow.Sent);                 // the observer never unblocked
        Assert.Equal(200 * ChunkLen, session.BodyLength);
    }

    [Fact]
    public async Task StuckObserver_DoesNotDelayAHealthyOne()
    {
        var options = new RelayOptions();
        var store = new RelayStore(options);
        var source = new FakeSocket();
        var session = await SeededSessionAsync(options, store, "queue_healthy", source);

        var slow = new BlockingSocket();
        var healthy = new FakeSocket();
        Assert.True(session.AddObserver(slow, priority: true));
        Assert.True(session.AddObserver(healthy, priority: true));

        await AppendAsync(session, source, 20);
        await healthy.WaitQuietAsync();

        Assert.Equal(0, slow.Sent);
        CheckExactlyOnce("healthy observer served while another is stuck", healthy, 20 * ChunkLen);
    }

    [Fact]
    public async Task QueueOverflow_DropsTheObserverAndClosesTheSocket()
    {
        var options = new RelayOptions { ObserverQueueFrames = 8 };
        var store = new RelayStore(options);
        var source = new FakeSocket();
        var session = await SeededSessionAsync(options, store, "queue_overflow", source);

        var slow = new BlockingSocket();
        Assert.True(session.AddObserver(slow, priority: true));
        Assert.Equal(1, session.ObserverCount);

        await AppendAsync(session, source, 200);    // far past the 8-frame queue

        await WaitForAsync(() => session.ObserverCount == 0, "the stuck observer to be dropped");
        // The socket is closed so the watch loop, blocked in ReceiveAsync, wakes and winds down.
        await WaitForAsync(() => slow.Closed, "the dropped observer's socket to be closed");
        Assert.Equal(0, slow.Sent);
    }

    [Fact]
    public async Task DroppedObserver_IsReportedToGo()
    {
        var options = new RelayOptions
        {
            GoObserversUrl = "http://go/observers",
            GoApiKey = "relay-key",
            ObserverChangeTimeout = 1,
            ObserverQueueFrames = 8,
        };
        var handler = new FakeGoHandler();
        var store = new RelayStore(options);
        using var http = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(5) };
        _ = new GoReporter(options, store, http, NullLogger<GoReporter>.Instance, CancellationToken.None);

        var session = store.GetOrCreateSession("queue_report");
        Assert.NotNull(session);
        session.SetHeaderReceivedForTest(new byte[HeaderLen]);
        var source = new FakeSocket();

        var slow = new BlockingSocket();
        Assert.True(session.AddObserver(slow, priority: true));
        // Let the join land first: an add and a drop inside one batch window would coalesce,
        // which would prove nothing about the drop reporting itself.
        await WaitForAsync(() => handler.RequestBodies.Count >= 1, "the join to be reported");
        Assert.Contains("\"observer_count\":1", handler.RequestBodies[0]);

        await AppendAsync(session, source, 200);
        await WaitForAsync(() => session.ObserverCount == 0, "the stuck observer to be dropped");

        // The watch loop's own RemoveObserverAsync finds it already gone and stays silent, and
        // the periodic flush returns early on empty dirty sets — so if the drop itself does not
        // mark the lobby, GO keeps counting a viewer that left.
        await WaitForAsync(() => handler.RequestBodies.Count >= 2, "the drop to be reported");
        Assert.Contains("\"observer_count\":0", handler.RequestBodies[^1]);
        Assert.Contains("\"is_live\":true", handler.RequestBodies[^1]);
    }

    [Fact]
    public async Task RejoinAfterDrop_IsReservedFromTheBody()
    {
        var options = new RelayOptions { ObserverQueueFrames = 8 };
        var store = new RelayStore(options);
        var source = new FakeSocket();
        var session = await SeededSessionAsync(options, store, "queue_rejoin", source);

        var slow = new BlockingSocket();
        Assert.True(session.AddObserver(slow, priority: true));
        await AppendAsync(session, source, 200);
        await WaitForAsync(() => session.ObserverCount == 0, "the stuck observer to be dropped");

        // A dropped observer's hole is only healed by reconnecting: catch-up covers the whole
        // body from the start, contiguously, exactly once.
        var rejoin = new FakeSocket();
        Assert.True(session.AddObserver(rejoin, priority: true));
        await AppendAsync(session, source, 5);
        await rejoin.WaitQuietAsync();

        Assert.Equal(BinaryEnvelope.MsgRole, rejoin.Frames[0].Type);
        Assert.Equal(BinaryEnvelope.MsgHeader, rejoin.Frames[1].Type);
        CheckExactlyOnce("rejoin re-served from the body", rejoin, 205 * ChunkLen);
        Assert.Equal(1, session.ObserverCount);
    }
}
