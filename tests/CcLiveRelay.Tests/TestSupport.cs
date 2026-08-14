using System.Net;
using System.Text.Json;
using CcLiveRelay.Protocol;
using CcLiveRelay.Session;
using CcLiveRelay.Util;

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace CcLiveRelay.Tests;

/// <summary>
/// Recorded-frames fake for IClientSocket, mirroring test_session_unit.py's FakeWS.
/// Frames are written by the per-observer writer task, so assertions must wait for the
/// queue to drain — see WaitQuietAsync.
/// </summary>
public sealed class FakeSocket : IClientSocket
{
    public List<(byte Type, byte[] Payload)> Frames { get; } = [];

    public bool IsOpen => true;

    public Task SendAsync(byte[] frame)
    {
        BinaryEnvelope.TryUnpackFrame(frame, out byte type, out byte[] payload);
        Frames.Add((type, payload));
        return Task.CompletedTask;
    }

    public Task CloseAsync() => Task.CompletedTask;

    /// <summary>Wait until no new frames arrive for `quietMs` (the writer task drains
    /// asynchronously; this makes frame assertions deterministic).</summary>
    public async Task WaitQuietAsync(int quietMs = 100, int timeoutMs = 2000)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (DateTime.UtcNow < deadline)
        {
            int count = Frames.Count;
            await Task.Delay(quietMs);
            if (Frames.Count == count)
                return;
        }
    }
}

/// <summary>Deterministic replacement for TimeSource.Now (mirrors FakeClock/ClockPatch).</summary>
public sealed class FakeClock
{
    public FakeClock(double start = 1000.0) => Now = start;
    public double Now { get; set; }
    public void Tick(double dt) => Now += dt;

    public IDisposable Install()
    {
        var original = TimeSource.Now;
        TimeSource.Now = () => Now;
        return new Restorer(original);
    }

    private sealed class Restorer(Func<double> original) : IDisposable
    {
        public void Dispose() => TimeSource.Now = original;
    }
}

/// <summary>
/// Stub outbound handler for the GO progress POSTs (mirrors stubbing notify_lobby_progress).
/// Records request bodies; serves statuses from a queue (last one repeats).
/// </summary>
public sealed class FakeGoHandler : HttpMessageHandler
{
    public List<string> RequestBodies { get; } = [];
    public Queue<HttpStatusCode> Statuses { get; } = [];

    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
                                                           CancellationToken cancellationToken)
    {
        string body = request.Content?.ReadAsStringAsync(cancellationToken).Result ?? "";
        RequestBodies.Add(body);
        var status = Statuses.Count > 0 ? Statuses.Dequeue() : HttpStatusCode.OK;
        return Task.FromResult(new HttpResponseMessage(status));
    }

    public List<JsonElement> EntryArrays()
    {
        var result = new List<JsonElement>();
        foreach (var body in RequestBodies)
        {
            using var doc = JsonDocument.Parse(body);
            result.Add(doc.RootElement.Clone());
        }
        return result;
    }
}

/// <summary>Shared environment for WebApplicationFactory tests: the relay reads env directly.</summary>
public static class TestEnv
{
    private static bool _configured;

    public static void Configure()
    {
        if (_configured)
            return;
        _configured = true;
        Environment.SetEnvironmentVariable("INTERNAL_API_KEY", "test123");
        Environment.SetEnvironmentVariable("GO_OBSERVERS_URL", "");
        Environment.SetEnvironmentVariable("GO_API_KEY", "");
        Environment.SetEnvironmentVariable("PUBLIC_WS_SCHEME", "wss");
        Environment.SetEnvironmentVariable("PUBLIC_HOST", "");
    }
}
