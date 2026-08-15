using System.Buffers.Binary;
using System.Text;
using CcLiveRelay.Config;
using CcLiveRelay.Protocol;
using CcLiveRelay.Session;
using CcLiveRelay.State;
using Xunit;

namespace CcLiveRelay.Tests;

/// <summary>
/// Unit tests for GameSession delivery semantics — the port of tests/test_session_unit.py.
/// Exercises the observer-join interleavings directly: observers write each BODY chunk at its
/// absolute file offset, so a chunk delivered out of order leaves a hole, and a chunk
/// delivered twice rewinds the client's parse cursor.
///
/// Delivery is asynchronous now: each observer has a writer queue drained by a background
/// task (see ObserverWriter), so every frame assertion is preceded by WaitQuietAsync.
/// </summary>
public class SessionUnitTests
{
    private const int HeaderLen = 40;
    private const int InitialBody = 100;
    private const int LiveChunk = 20;
    private const int DelaySeconds = 42;

    private static byte[] U64(long value)
    {
        var buf = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(buf, (ulong)value);
        return buf;
    }

    private static byte[] U32(uint value) => BinaryEnvelope.PackU32(value);

    private static byte[] Fill(int count, byte fill) => Enumerable.Repeat(fill, count).ToArray();

    private sealed class SessionFixture
    {
        public RelayStore Store { get; }
        public GameSession Session { get; }
        public FakeSocket Source { get; }

        public SessionFixture(RelayOptions? options = null, string lobbyId = "unittest")
        {
            options ??= new RelayOptions();
            Store = new RelayStore(options);
            Session = new GameSession(lobbyId, options, Store);
            Source = new FakeSocket();
        }

        public async Task SeedAsync()
        {
            await Session.ApplyHeaderAsync(Source, Fill(HeaderLen, (byte)'H'));
            await Session.ApplyBodyAsync(Source, Concat(U64(0), Fill(InitialBody, (byte)'A')));
            Session.DelaySeconds = DelaySeconds;
        }
    }

    private static byte[] Concat(params byte[][] parts)
    {
        var result = new byte[parts.Sum(p => p.Length)];
        int offset = 0;
        foreach (var part in parts)
        {
            part.CopyTo(result, offset);
            offset += part.Length;
        }
        return result;
    }

    /// <summary>File-offset spans of BODY frames, in delivery order.</summary>
    private static List<(long Start, long End)> BodySpans(FakeSocket ws)
    {
        var spans = new List<(long, long)>();
        foreach (var (type, payload) in ws.Frames)
        {
            if (type != BinaryEnvelope.MsgBody)
                continue;
            long offset = (long)BinaryPrimitives.ReadUInt64LittleEndian(payload.AsSpan(0, 8));
            spans.Add((offset, offset + payload.Length - 8));
        }
        return spans;
    }

    private static void CheckExactlyOnce(string label, FakeSocket ws, int totalBody)
    {
        var spans = BodySpans(ws);
        long cursor = HeaderLen;
        foreach (var (start, end) in spans)
        {
            if (start != cursor)
                throw new Xunit.Sdk.XunitException(
                    $"{label}: {(start < cursor ? "overlap" : "gap")}: chunk at {start}, expected {cursor} (spans={string.Join(";", spans)})");
            cursor = end;
        }
        Assert.True(cursor == HeaderLen + totalBody,
            $"{label}: covered to {cursor}, expected {HeaderLen + totalBody} (spans={string.Join(";", spans)})");
    }

    private static List<uint> TickFrames(FakeSocket ws) =>
        ws.Frames.Where(f => f.Type == BinaryEnvelope.MsgTick)
           .Select(f => BinaryPrimitives.ReadUInt32LittleEndian(f.Payload.AsSpan(0, 4)))
           .ToList();

    private static FakeSocket RegisterSource(GameSession session, long bodyLenSeen = 0)
    {
        var ws = new FakeSocket();
        session.AddSource(ws, false, bodyLenSeen);
        return ws;
    }

    // ── Observer-join interleavings ─────────────────────────────────────────

    [Fact]
    public async Task JoinThenAppend_DeliversExactlyOnce()
    {
        var fx = new SessionFixture();
        await fx.SeedAsync();
        var ws = new FakeSocket();

        // Fire a live append concurrently with the join: registration + catch-up are one
        // atomic step under the session lock, so the append's broadcast can only be enqueued
        // before registration (observer not yet targeted) or after the catch-up — never
        // interleaved with it.
        var append = fx.Session.ApplyBodyAsync(fx.Source,
            Concat(U64(InitialBody), Fill(LiveChunk, (byte)'B')));
        Assert.True(fx.Session.AddObserver(ws, priority: true));
        await append;
        await ws.WaitQuietAsync();

        var types = ws.Frames.Select(f => f.Type).ToList();
        Assert.Equal(new byte[] { BinaryEnvelope.MsgRole }, types.Take(1));
        Assert.Equal(BinaryEnvelope.MsgHeader, types[1]);
        Assert.Contains($"\"delay_seconds\":{DelaySeconds}",
            Encoding.UTF8.GetString(ws.Frames[0].Payload));
        CheckExactlyOnce("body delivered exactly once", ws, InitialBody + LiveChunk);
    }

    [Fact]
    public async Task AppendThenJoin_DeliversExactlyOnce()
    {
        var fx = new SessionFixture();
        await fx.SeedAsync();
        await fx.Session.ApplyBodyAsync(fx.Source,
            Concat(U64(InitialBody), Fill(LiveChunk, (byte)'B')));
        var ws = new FakeSocket();

        Assert.True(fx.Session.AddObserver(ws, priority: true));
        await ws.WaitQuietAsync();

        CheckExactlyOnce("body delivered exactly once", ws, InitialBody + LiveChunk);
    }

    // ── Byte-level delay hold ───────────────────────────────────────────────

    [Fact]
    public async Task DelayedWatermark_ReturnsBodyLengthAsOfNowMinusDelay()
    {
        var fx = new SessionFixture();
        fx.Session.DelaySeconds = DelaySeconds;
        for (int i = 0; i < 5; i++)
        {
            using var clock = new FakeClock(1010 + i * 10).Install();
            await fx.Session.ApplyBodyAsync(fx.Source,
                Concat(U64(20L * i), Fill(20, (byte)'B')));
        }

        Assert.Equal(0, fx.Session.DelayedWatermark(1042));
        Assert.Equal(20, fx.Session.DelayedWatermark(1052));
        Assert.Equal(40, fx.Session.DelayedWatermark(1065));
        Assert.Equal(100, fx.Session.DelayedWatermark(1099));

        fx.Session.DelaySeconds = 0;
        Assert.Equal(100, fx.Session.DelayedWatermark(1042));
    }

    [Fact]
    public async Task HeldObserver_JoinsAtDelayedEdgeAndReceivesAtPlusDelay()
    {
        var clock = new FakeClock();
        using (clock.Install())
        {
            var fx = new SessionFixture();
            fx.Session.DelaySeconds = DelaySeconds;
            await fx.Session.ApplyHeaderAsync(fx.Source, Fill(HeaderLen, (byte)'H'));

            // Five chunks recorded at t=1002..1010 (body = 100); observer joins at t=1010.
            for (int i = 0; i < 5; i++)
            {
                clock.Now = 1000 + (i + 1) * 2;
                await fx.Session.ApplyBodyAsync(fx.Source,
                    Concat(U64(20L * i), Fill(20, (byte)'B')));
            }
            var ws = new FakeSocket();
            Assert.True(fx.Session.AddObserver(ws, priority: false));
            await ws.WaitQuietAsync();

            Assert.Contains("\"delay_seconds\":0",
                Encoding.UTF8.GetString(ws.Frames[0].Payload));
            Assert.Empty(ws.Frames.Where(f => f.Type == BinaryEnvelope.MsgBody));

            // A chunk appended after join (t=1012, ready t=1054) must not be delivered yet.
            clock.Now = 1012;
            await fx.Session.ApplyBodyAsync(fx.Source, Concat(U64(100), Fill(20, (byte)'B')));
            await ws.WaitQuietAsync();
            Assert.Empty(ws.Frames.Where(f => f.Type == BinaryEnvelope.MsgBody));

            // The shared edge advances with the clock: watermark(1045)=20, (1047)=40, (1052)=100.
            clock.Tick(33);
            fx.Session.FlushHeldObservers();
            clock.Tick(2);
            fx.Session.FlushHeldObservers();
            clock.Tick(5);
            fx.Session.FlushHeldObservers();
            await ws.WaitQuietAsync();
            CheckExactlyOnce("delayed edge delivered 0-100", ws, 100);

            // The post-join chunk becomes available at its own arrival + delay (1054).
            clock.Now = 1055;
            fx.Session.FlushHeldObservers();
            await ws.WaitQuietAsync();
            CheckExactlyOnce("all 120 bytes delivered exactly once", ws, 120);
        }
    }

    [Fact]
    public async Task PriorityObserver_NotHeld()
    {
        var clock = new FakeClock();
        using (clock.Install())
        {
            var fx = new SessionFixture();
            fx.Session.DelaySeconds = DelaySeconds;
            await fx.Session.ApplyHeaderAsync(fx.Source, Fill(HeaderLen, (byte)'H'));
            for (int i = 0; i < 5; i++)
            {
                clock.Now = 1000 + (i + 1) * 2;
                await fx.Session.ApplyBodyAsync(fx.Source,
                    Concat(U64(20L * i), Fill(20, (byte)'B')));
            }
            var ws = new FakeSocket();
            Assert.True(fx.Session.AddObserver(ws, priority: true));
            await ws.WaitQuietAsync();

            Assert.Contains($"\"delay_seconds\":{DelaySeconds}",
                Encoding.UTF8.GetString(ws.Frames[0].Payload));
            clock.Now = 1012;
            await fx.Session.ApplyBodyAsync(fx.Source, Concat(U64(100), Fill(20, (byte)'B')));
            await ws.WaitQuietAsync();
            CheckExactlyOnce("priority observer got everything immediately", ws, 120);
        }
    }

    [Fact]
    public async Task HeldObserver_EndFlushesRemainingBytes()
    {
        var clock = new FakeClock();
        using (clock.Install())
        {
            var fx = new SessionFixture();
            fx.Session.DelaySeconds = DelaySeconds;
            await fx.Session.ApplyHeaderAsync(fx.Source, Fill(HeaderLen, (byte)'H'));
            for (int i = 0; i < 5; i++)
            {
                clock.Now = 1000 + (i + 1) * 2;
                await fx.Session.ApplyBodyAsync(fx.Source,
                    Concat(U64(20L * i), Fill(20, (byte)'B')));
            }
            var ws = new FakeSocket();
            Assert.True(fx.Session.AddObserver(ws, priority: false));
            await ws.WaitQuietAsync();

            clock.Now = 1012;   // not yet due (ready at 1054)
            await fx.Session.ApplyBodyAsync(fx.Source, Concat(U64(100), Fill(20, (byte)'B')));
            fx.Session.BroadcastEnvelope(BinaryEnvelope.MsgEnd, [], [ws]);
            await ws.WaitQuietAsync();

            CheckExactlyOnce("END flushed the whole body", ws, 120);
            Assert.Equal(1, ws.Frames.Count(f => f.Type == BinaryEnvelope.MsgEnd));
        }
    }

    [Fact]
    public async Task DelayZero_NotHeld()
    {
        var fx = new SessionFixture();
        await fx.SeedAsync();
        fx.Session.DelaySeconds = 0;
        var ws = new FakeSocket();

        Assert.True(fx.Session.AddObserver(ws, priority: false));
        await ws.WaitQuietAsync();

        await fx.Session.ApplyBodyAsync(fx.Source, Concat(U64(InitialBody), Fill(LiveChunk, (byte)'B')));
        await ws.WaitQuietAsync();
        CheckExactlyOnce("delay 0 -> everything delivered", ws, InitialBody + LiveChunk);
    }

    // ── Frame heartbeat (MSG_TICK) ──────────────────────────────────────────

    [Fact]
    public async Task Tick_ForwardedToLiveObserver()
    {
        var fx = new SessionFixture();
        await fx.SeedAsync();
        fx.Session.DelaySeconds = 0;
        var ws = new FakeSocket();

        Assert.True(fx.Session.AddObserver(ws, priority: false));

        await fx.Session.ApplyTickAsync(fx.Source, U32(600));
        await fx.Session.ApplyTickAsync(fx.Source, U32(610));
        await ws.WaitQuietAsync();

        Assert.Equal([600u, 610u], TickFrames(ws));
        Assert.Equal(610u, fx.Session.LastTickFrame);
    }

    [Fact]
    public async Task Tick_NotForwardedToHeldObserverBeforeDelayElapses()
    {
        // The raw live tick must never reach a held observer (it states the true, undelayed
        // frame) - only the delayed value from DelayedTickFrame, which stays 0 until a tick has
        // aged past the delay. This test covers "immediately after the tick, before any time
        // has passed"; Tick_ForwardedToHeldObserverAfterDelayElapses covers the other side.
        var fx = new SessionFixture();
        await fx.SeedAsync();
        fx.Session.DelaySeconds = DelaySeconds;
        var ws = new FakeSocket();

        Assert.True(fx.Session.AddObserver(ws, priority: false));

        await fx.Session.ApplyTickAsync(fx.Source, U32(600));
        await ws.WaitQuietAsync();

        Assert.Empty(TickFrames(ws));
        Assert.Equal(600u, fx.Session.LastTickFrame);
    }

    [Fact]
    public async Task DelayedTickFrame_ReturnsTickAsOfNowMinusDelay()
    {
        var fx = new SessionFixture();
        fx.Session.DelaySeconds = DelaySeconds;
        for (int i = 0; i < 3; i++)
        {
            using var clock = new FakeClock(1010 + i * 10).Install();
            await fx.Session.ApplyTickAsync(fx.Source, U32(600u + (uint)(i * 10)));
        }
        // Ticks recorded at t=1010(600), t=1020(610), t=1030(620); delay=42.
        Assert.Equal(0u, fx.Session.DelayedTickFrame(1051));   // before the first ages in
        Assert.Equal(600u, fx.Session.DelayedTickFrame(1052));
        Assert.Equal(610u, fx.Session.DelayedTickFrame(1065));
        Assert.Equal(620u, fx.Session.DelayedTickFrame(1099));

        fx.Session.DelaySeconds = 0;
        Assert.Equal(620u, fx.Session.DelayedTickFrame(1052));
    }

    [Fact]
    public async Task Tick_ForwardedToHeldObserverAfterDelayElapses()
    {
        // The bug this guards against (2026-08-15): held observers were withheld from the
        // heartbeat entirely, so their only "what frame is the game at" signal was the raw
        // record edge, which sawtooths whenever a stretch of play produces no records - the
        // client-side buffering gate would then freeze for that whole stretch. The fix is a
        // delayed tick, bound by the exact same watermark boundary as body bytes, so it can
        // never reveal anything the observer couldn't already derive from bytes it's legitimately
        // receiving.
        var clock = new FakeClock(1000);
        using (clock.Install())
        {
            var fx = new SessionFixture();
            fx.Session.DelaySeconds = DelaySeconds;
            await fx.Session.ApplyHeaderAsync(fx.Source, Fill(HeaderLen, (byte)'H'));

            var ws = new FakeSocket();
            Assert.True(fx.Session.AddObserver(ws, priority: false));
            await ws.WaitQuietAsync();

            clock.Now = 1010;
            await fx.Session.ApplyTickAsync(fx.Source, U32(600));
            await ws.WaitQuietAsync();
            Assert.Empty(TickFrames(ws));   // not aged past the delay yet

            clock.Now = 1010 + DelaySeconds + 1;
            fx.Session.FlushHeldObservers();
            await ws.WaitQuietAsync();

            Assert.Equal([600u], TickFrames(ws));   // the delayed value, not any later live tick
        }
    }

    [Fact]
    public async Task Tick_Monotonic()
    {
        var fx = new SessionFixture();
        await fx.SeedAsync();
        fx.Session.DelaySeconds = 0;
        var ws = new FakeSocket();

        Assert.True(fx.Session.AddObserver(ws, priority: false));

        await fx.Session.ApplyTickAsync(fx.Source, U32(900));
        await fx.Session.ApplyTickAsync(fx.Source, U32(800));   // laggier source
        await fx.Session.ApplyTickAsync(fx.Source, U32(900));   // duplicate
        await ws.WaitQuietAsync();

        Assert.Equal([900u], TickFrames(ws));
        Assert.Equal(900u, fx.Session.LastTickFrame);
    }

    [Fact]
    public async Task Tick_InCatchup_AfterBody()
    {
        var fx = new SessionFixture();
        await fx.SeedAsync();
        fx.Session.DelaySeconds = 0;
        await fx.Session.ApplyTickAsync(fx.Source, U32(750));

        var ws = new FakeSocket();
        Assert.True(fx.Session.AddObserver(ws, priority: false));
        await ws.WaitQuietAsync();

        Assert.Equal([750u], TickFrames(ws));
        var types = ws.Frames.Select(f => f.Type).ToList();
        Assert.Contains(BinaryEnvelope.MsgBody, types);
        int lastBody = types.FindLastIndex(t => t == BinaryEnvelope.MsgBody);
        int tick = types.IndexOf(BinaryEnvelope.MsgTick);
        Assert.True(tick > lastBody, "tick follows the catch-up body");
    }

    [Fact]
    public async Task Tick_IgnoredFromDemotedSource()
    {
        var fx = new SessionFixture();
        await fx.SeedAsync();
        fx.Session.DelaySeconds = 0;
        var ws = new FakeSocket();

        Assert.True(fx.Session.AddObserver(ws, priority: false));

        var src = RegisterSource(fx.Session);
        fx.Session.GetSourceHealth(src)!.Demoted = true;

        await fx.Session.ApplyTickAsync(src, U32(600));
        await ws.WaitQuietAsync();

        Assert.Empty(TickFrames(ws));
        Assert.Equal(0u, fx.Session.LastTickFrame);
    }

    // ── All-push demotion / re-promotion ────────────────────────────────────

    [Fact]
    public async Task DemoteOnLagThreshold()
    {
        var opts = new RelayOptions { SourceLagBytes = LiveChunk * 5 };
        var fx = new SessionFixture(opts);
        await fx.SeedAsync();
        var a = RegisterSource(fx.Session);
        var b = RegisterSource(fx.Session);

        // A streams live; B always arrives exactly one chunk behind (lag == 20 per frame).
        for (int i = 0; i < 10; i++)
        {
            var chunk = Concat(U64(InitialBody + i * LiveChunk), Fill(LiveChunk, (byte)'B'));
            await fx.Session.ApplyBodyAsync(a, chunk);
            fx.Session.TouchSource(a);
            await fx.Session.ApplyBodyAsync(b, chunk);
            fx.Session.TouchSource(b);
        }

        Assert.Equal(10L * LiveChunk, fx.Session.GetSourceHealth(b)!.LagBytes);
        Assert.Equal(0L, fx.Session.GetSourceHealth(a)!.LagBytes);

        Assert.True(await fx.Session.MaybeDemoteSourceAsync(b));
        Assert.True(fx.Session.GetSourceHealth(b)!.Demoted);

        // Demoted source's frames are ignored.
        int before = fx.Session.BodyLength;
        await fx.Session.ApplyBodyAsync(b, Concat(U64(before), Fill(LiveChunk, (byte)'X')));
        Assert.Equal(before, fx.Session.BodyLength);

        // A still streams.
        await fx.Session.ApplyBodyAsync(a, Concat(U64(before), Fill(LiveChunk, (byte)'C')));
        Assert.Equal(before + LiveChunk, fx.Session.BodyLength);
    }

    [Fact]
    public async Task DemoteOnGapStrikes()
    {
        var fx = new SessionFixture();
        await fx.SeedAsync();
        var a = RegisterSource(fx.Session);
        var b = RegisterSource(fx.Session);

        for (int i = 0; i < fx.Store.Options.SourceGapStrikes; i++)
        {
            await fx.Session.ApplyBodyAsync(b,
                Concat(U64(InitialBody + 1000 + i * 100), Fill(10, (byte)'G')));
            fx.Session.TouchSource(b);
        }

        Assert.Equal(fx.Store.Options.SourceGapStrikes,
            fx.Session.GetSourceHealth(b)!.GapStrikes);
        Assert.True(await fx.Session.MaybeDemoteSourceAsync(b));
    }

    [Fact]
    public async Task NeverDemoteLastActiveSource()
    {
        var fx = new SessionFixture();
        await fx.SeedAsync();
        var a = RegisterSource(fx.Session);

        fx.Session.GetSourceHealth(a)!.GapStrikes = 999;
        fx.Session.GetSourceHealth(a)!.LagBytes = 10_000_000;

        Assert.False(await fx.Session.MaybeDemoteSourceAsync(a));
        Assert.False(fx.Session.GetSourceHealth(a)!.Demoted);

        var b = RegisterSource(fx.Session);
        Assert.True(await fx.Session.MaybeDemoteSourceAsync(a));
    }

    [Fact]
    public async Task PromoteBackup_WhenActiveLeaves()
    {
        var opts = new RelayOptions { SourceLagBytes = LiveChunk * 5 };
        var fx = new SessionFixture(opts);
        await fx.SeedAsync();
        var a = RegisterSource(fx.Session);
        var b = RegisterSource(fx.Session);

        fx.Session.GetSourceHealth(b)!.LagBytes = 50 * LiveChunk;
        await fx.Session.ApplyBodyAsync(a, Concat(U64(InitialBody), Fill(LiveChunk, (byte)'B')));
        fx.Session.TouchSource(a);

        Assert.True(await fx.Session.MaybeDemoteSourceAsync(b));

        // A (the only active pusher) leaves.
        await fx.Session.RemoveSourceAsync(a);

        Assert.False(fx.Session.GetSourceHealth(b)!.Demoted);
        var roleFrame = b.Frames.Where(f => f.Type == BinaryEnvelope.MsgRole)
                                .Select(f => Encoding.UTF8.GetString(f.Payload))
                                .LastOrDefault();
        Assert.Contains("takeover", roleFrame);
        Assert.Contains($"\"body_offset\":{InitialBody + LiveChunk}", roleFrame);
    }

    [Fact]
    public async Task PromotePicks_LeastBadBackup()
    {
        var opts = new RelayOptions { SourceLagBytes = LiveChunk * 5 };
        var fx = new SessionFixture(opts);
        await fx.SeedAsync();
        var a = RegisterSource(fx.Session);
        var good = RegisterSource(fx.Session);
        var bad = RegisterSource(fx.Session);

        fx.Session.GetSourceHealth(good)!.LagBytes = 5 * LiveChunk;
        fx.Session.GetSourceHealth(bad)!.LagBytes = 200 * LiveChunk;
        Assert.True(await fx.Session.MaybeDemoteSourceAsync(good));
        Assert.True(await fx.Session.MaybeDemoteSourceAsync(bad));

        await fx.Session.RemoveSourceAsync(a);

        Assert.False(fx.Session.GetSourceHealth(good)!.Demoted);
        Assert.True(fx.Session.GetSourceHealth(bad)!.Demoted);
    }

    [Fact]
    public async Task NoPromotion_WhenActiveRemains()
    {
        var fx = new SessionFixture();
        await fx.SeedAsync();
        var a = RegisterSource(fx.Session);
        _ = RegisterSource(fx.Session);

        // A source leaving while another active pusher remains does not promote anyone.
        Assert.False(await fx.Session.MaybePromoteBackupAsync());

        await fx.Session.RemoveSourceAsync(a);
        Assert.False(fx.Session.Ended);
    }
}
