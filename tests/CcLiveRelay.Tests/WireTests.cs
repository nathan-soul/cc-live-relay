using System.Buffers.Binary;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using CcLiveRelay.Protocol;
using CcLiveRelay.Session;
using CcLiveRelay.State;
using Xunit;

namespace CcLiveRelay.Tests;

/// <summary>
/// Real-socket flows over the in-process TestServer — the port of the wire-level parts of
/// tests/test_relay.py. Uses TestServer's WebSocket client so nothing listens on a port.
/// </summary>
public class WireTests : IClassFixture<WireTests.App>
{
    public sealed class App : Microsoft.AspNetCore.Mvc.Testing.WebApplicationFactory<Program>
    {
        static App() => TestEnv.Configure();
    }

    private readonly App _app;

    public WireTests(App app) => _app = app;

    private HttpClient Keys()
    {
        var client = _app.CreateClient();
        client.DefaultRequestHeaders.Add("X-Relay-Key", "test123");
        return client;
    }

    private async Task<string> CreateLivestreamAsync(string lobbyId, long ownerUserId = 1,
                                                     int? delaySeconds = null)
    {
        string delay = delaySeconds is null ? "" : $",\"delay_seconds\":{delaySeconds}";
        var response = await Keys().PostAsync("/internal/livestreams",
            new StringContent($"{{\"lobby_id\":\"{lobbyId}\",\"owner_user_id\":{ownerUserId}{delay}}}",
                Encoding.UTF8, "application/json"));
        Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);
        return lobbyId;
    }

    private async Task<string> MintStreamTokenAsync(string lobbyId, long userId)
    {
        var response = await Keys().PostAsync("/internal/stream_tokens",
            new StringContent($"{{\"lobby_id\":\"{lobbyId}\",\"user_id\":{userId}}}",
                Encoding.UTF8, "application/json"));
        Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        return doc.RootElement.GetProperty("url").GetString()!.Split('=')[1];
    }

    private async Task<string> MintWatchTicketAsync(string lobbyId, long userId, bool priority = false)
    {
        var response = await Keys().PostAsync("/internal/watch_tickets",
            new StringContent($"{{\"lobby_id\":\"{lobbyId}\",\"user_id\":{userId}," +
                              $"\"priority\":{(priority ? "true" : "false")}}}",
                Encoding.UTF8, "application/json"));
        Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        return doc.RootElement.GetProperty("url").GetString()!.Split('=')[1];
    }

    private async Task<System.Net.WebSockets.WebSocket> ConnectAsync(string pathAndQuery)
    {
        var client = _app.Server.CreateWebSocketClient();
        return await client.ConnectAsync(new Uri($"ws://localhost{pathAndQuery}"),
            CancellationToken.None);
    }

    private static byte[] U64(long value)
    {
        var buf = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(buf, (ulong)value);
        return buf;
    }

    private sealed class FrameCollector
    {
        private readonly System.Net.WebSockets.WebSocket _ws;
        private readonly EnvelopeReader _reader = new();
        private readonly byte[] _buffer = new byte[64 * 1024];

        public FrameCollector(System.Net.WebSockets.WebSocket ws) => _ws = ws;

        /// <summary>Read frames until predicate matches, or fail after the timeout.</summary>
        public async Task<(byte Type, byte[] Payload)> ReadUntilAsync(
            Func<byte, byte[], bool> predicate, int timeoutMs = 5000)
        {
            using var cts = new CancellationTokenSource(timeoutMs);
            while (true)
            {
                while (_reader.TryReadFrame(out byte type, out byte[] payload))
                {
                    if (predicate(type, payload))
                        return (type, payload);
                }
                WebSocketReceiveResult result;
                try
                {
                    result = await _ws.ReceiveAsync(_buffer, cts.Token);
                }
                catch (OperationCanceledException)
                {
                    throw new Xunit.Sdk.XunitException("timed out waiting for a matching frame");
                }
                if (result.MessageType == WebSocketMessageType.Close)
                    throw new Xunit.Sdk.XunitException("websocket closed while waiting");
                _reader.Append(_buffer.AsMemory(0, result.Count));
            }
        }

        /// <summary>True if any bytes arrive within the window; the "expect silence" probe.</summary>
        public async Task<bool> ReadAnyAsync(TimeSpan window)
        {
            using var cts = new CancellationTokenSource(window);
            while (true)
            {
                if (_reader.BufferedBytes > 0)
                    return true;
                WebSocketReceiveResult result;
                try
                {
                    result = await _ws.ReceiveAsync(_buffer, cts.Token);
                }
                catch (OperationCanceledException)
                {
                    return _reader.BufferedBytes > 0;
                }
                if (result.MessageType == WebSocketMessageType.Close)
                    return false;
                _reader.Append(_buffer.AsMemory(0, result.Count));
            }
        }

        /// <summary>Frame types currently buffered (diagnostic only).</summary>
        public List<(byte Type, int Len)> DrainBufferedTypes()
        {
            var types = new List<(byte, int)>();
            while (_reader.TryReadFrame(out byte type, out byte[] payload))
                types.Add((type, payload.Length));
            return types;
        }
    }

    [Fact]
    public async Task StreamerToObserver_LiveFlowWithTicks()
    {
        string lobbyId = "wire_001";
        await CreateLivestreamAsync(lobbyId, ownerUserId: 1, delaySeconds: 0);
        string token = await MintStreamTokenAsync(lobbyId, userId: 1);
        string ticket = await MintWatchTicketAsync(lobbyId, userId: 2, priority: true);

        // ── Streamer connects and registers ────────────────────────────────
        var streamer = await ConnectAsync($"/stream/{lobbyId}?stream_token={token}");
        var streamerCollector = new FrameCollector(streamer);

        var regPayload = JsonSerializer.Serialize(new
        {
            lobbyid = lobbyId,
            can_stream = true,
            player_name = "Host",
            is_observer = false,
            lobby = new { name = "test map", members = new object[] { } },
        });
        await streamer.SendAsync(
            BinaryEnvelope.Pack(BinaryEnvelope.MsgRegister, Encoding.UTF8.GetBytes(regPayload)),
            WebSocketMessageType.Binary, true, CancellationToken.None);

        var role = await streamerCollector.ReadUntilAsync(
            (t, _) => t == BinaryEnvelope.MsgRole);
        Assert.Contains("\"role\":\"streamer\"", Encoding.UTF8.GetString(role.Payload));

        // ── Header + body + tick ───────────────────────────────────────────
        await streamer.SendAsync(BinaryEnvelope.Pack(BinaryEnvelope.MsgHeader, new byte[40]),
            WebSocketMessageType.Binary, true, CancellationToken.None);
        await streamer.SendAsync(
            BinaryEnvelope.Pack(BinaryEnvelope.MsgBody, U64(0).Concat(Enumerable.Repeat((byte)'A', 100)).ToArray()),
            WebSocketMessageType.Binary, true, CancellationToken.None);
        await streamer.SendAsync(
            BinaryEnvelope.Pack(BinaryEnvelope.MsgTick, BinaryEnvelope.PackU32(600)),
            WebSocketMessageType.Binary, true, CancellationToken.None);

        // ── Observer connects (priority: no delay hold) ────────────────────
        var observer = await ConnectAsync($"/watch/{lobbyId}?ticket={ticket}");
        try
        {
            var observerCollector = new FrameCollector(observer);

            var config = await observerCollector.ReadUntilAsync(
                (t, _) => t == BinaryEnvelope.MsgRole);
            Assert.Contains("\"delay_seconds\":0", Encoding.UTF8.GetString(config.Payload));

            var header = await observerCollector.ReadUntilAsync(
                (t, _) => t == BinaryEnvelope.MsgHeader);
            Assert.Equal(40, header.Payload.Length);

            var body = await observerCollector.ReadUntilAsync(
                (t, p) => t == BinaryEnvelope.MsgBody &&
                          BinaryPrimitives.ReadUInt64LittleEndian(p.AsSpan(0, 8)) == 40 &&
                          p.Length == 108);
            Assert.Equal(100, body.Payload.Length - 8);

            var tick = await observerCollector.ReadUntilAsync(
                (t, _) => t == BinaryEnvelope.MsgTick);
            Assert.Equal(600u, BinaryPrimitives.ReadUInt32LittleEndian(tick.Payload.AsSpan(0, 4)));

            // ── Live chunk + tick after join ───────────────────────────────────
            await streamer.SendAsync(
                BinaryEnvelope.Pack(BinaryEnvelope.MsgBody,
                    U64(100).Concat(Enumerable.Repeat((byte)'B', 20)).ToArray()),
                WebSocketMessageType.Binary, true, CancellationToken.None);
            await streamer.SendAsync(
                BinaryEnvelope.Pack(BinaryEnvelope.MsgTick, BinaryEnvelope.PackU32(610)),
                WebSocketMessageType.Binary, true, CancellationToken.None);

            var liveChunk = await observerCollector.ReadUntilAsync(
                (t, p) => t == BinaryEnvelope.MsgBody &&
                          BinaryPrimitives.ReadUInt64LittleEndian(p.AsSpan(0, 8)) == 140);
            Assert.Equal(20, liveChunk.Payload.Length - 8);
            var liveTick = await observerCollector.ReadUntilAsync(
                (t, _) => t == BinaryEnvelope.MsgTick);
            Assert.Equal(610u, BinaryPrimitives.ReadUInt32LittleEndian(liveTick.Payload.AsSpan(0, 4)));
        }
        finally
        {
            streamer.Abort();
            observer.Abort();
        }
    }

    [Fact]
    public async Task HeldObserver_GetsNoLiveBytesOrTicks()
    {
        string lobbyId = "wire_002";
        await CreateLivestreamAsync(lobbyId, ownerUserId: 1, delaySeconds: 5);
        string token = await MintStreamTokenAsync(lobbyId, userId: 1);
        string ticket = await MintWatchTicketAsync(lobbyId, userId: 2, priority: false);

        var streamer = await ConnectAsync($"/stream/{lobbyId}?stream_token={token}");
        var streamerCollector = new FrameCollector(streamer);
        var regPayload = JsonSerializer.Serialize(new { lobbyid = lobbyId, can_stream = true });
        await streamer.SendAsync(
            BinaryEnvelope.Pack(BinaryEnvelope.MsgRegister, Encoding.UTF8.GetBytes(regPayload)),
            WebSocketMessageType.Binary, true, CancellationToken.None);
        await streamerCollector.ReadUntilAsync((t, _) => t == BinaryEnvelope.MsgRole);

        await streamer.SendAsync(BinaryEnvelope.Pack(BinaryEnvelope.MsgHeader, new byte[40]),
            WebSocketMessageType.Binary, true, CancellationToken.None);
        await streamer.SendAsync(
            BinaryEnvelope.Pack(BinaryEnvelope.MsgBody, U64(0).Concat(Enumerable.Repeat((byte)'A', 100)).ToArray()),
            WebSocketMessageType.Binary, true, CancellationToken.None);
        await streamer.SendAsync(
            BinaryEnvelope.Pack(BinaryEnvelope.MsgTick, BinaryEnvelope.PackU32(600)),
            WebSocketMessageType.Binary, true, CancellationToken.None);

        // Held observer: catch-up is limited to the delayed edge (nothing older than 5 s),
        // live BODY/TICK frames are withheld, and the config says delay_seconds: 0.
        var observer = await ConnectAsync($"/watch/{lobbyId}?ticket={ticket}");
        try
        {
            var observerCollector = new FrameCollector(observer);

            var config = await observerCollector.ReadUntilAsync(
                (t, _) => t == BinaryEnvelope.MsgRole);
            Assert.Contains("\"delay_seconds\":0", Encoding.UTF8.GetString(config.Payload));

            var header = await observerCollector.ReadUntilAsync(
                (t, _) => t == BinaryEnvelope.MsgHeader);
            Assert.Equal(40, header.Payload.Length);

            // No live-edge data may reach a held observer: BODY/TICK/PATCH/CHAT frames are
            // withheld. (A duplicate HEADER is legitimate: the observer can register between
            // the header's broadcast and its catch-up snapshot, and writing the same header
            // twice is idempotent — the client's pre-roll starts at file offset 0 either way.)
            bool leaked = await observerCollector.ReadAnyAsync(TimeSpan.FromMilliseconds(500));
            if (leaked)
            {
                var leakedFrames = observerCollector.DrainBufferedTypes();
                bool hardLeak = leakedFrames.Any(f => f is (byte t, _) &&
                    (t == BinaryEnvelope.MsgBody || t == BinaryEnvelope.MsgTick ||
                     t == BinaryEnvelope.MsgPatch || t == BinaryEnvelope.MsgChat ||
                     t == BinaryEnvelope.MsgSpectatorChat));
                Assert.False(hardLeak, "held observer received live-edge data: " +
                                       string.Join(",", leakedFrames));
            }
        }
        finally
        {
            streamer.Abort();
            observer.Abort();
        }
    }

    [Fact]
    public async Task WatchWithoutTicket_Rejected()
    {
        string lobbyId = "wire_003";
        await CreateLivestreamAsync(lobbyId);
        var watchClient = _app.Server.CreateWebSocketClient();
        var ws = await watchClient.ConnectAsync(
            new Uri($"ws://localhost/watch/{lobbyId}"), CancellationToken.None);
        try
        {
            var collector = new FrameCollector(ws);
            var error = await collector.ReadUntilAsync((t, _) => t == BinaryEnvelope.MsgError);
            Assert.Contains("Missing or invalid watch ticket", Encoding.UTF8.GetString(error.Payload));
        }
        finally
        {
            ws.Dispose();
        }
    }

    [Fact]
    public async Task StreamToken_SingleUse()
    {
        string lobbyId = "wire_004";
        await CreateLivestreamAsync(lobbyId);
        string token = await MintStreamTokenAsync(lobbyId, userId: 1);

        var first = await ConnectAsync($"/stream/{lobbyId}?stream_token={token}");
        var regPayload = JsonSerializer.Serialize(new { lobbyid = lobbyId, can_stream = true });
        await first.SendAsync(
            BinaryEnvelope.Pack(BinaryEnvelope.MsgRegister, Encoding.UTF8.GetBytes(regPayload)),
            WebSocketMessageType.Binary, true, CancellationToken.None);
        var firstCollector = new FrameCollector(first);
        await firstCollector.ReadUntilAsync((t, _) => t == BinaryEnvelope.MsgRole);
        first.Abort();

        // The same token must not admit a second connection.
        var secondClient = _app.Server.CreateWebSocketClient();
        var second = await secondClient.ConnectAsync(
            new Uri($"ws://localhost/stream/{lobbyId}?stream_token={token}"),
            CancellationToken.None);
        try
        {
            var secondCollector = new FrameCollector(second);
            var error = await secondCollector.ReadUntilAsync((t, _) => t == BinaryEnvelope.MsgError);
            Assert.Contains("Invalid or expired stream token", Encoding.UTF8.GetString(error.Payload));
        }
        finally
        {
            second.Dispose();
        }
    }
}
