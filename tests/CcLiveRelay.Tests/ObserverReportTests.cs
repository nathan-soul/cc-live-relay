using System.Text.Json;
using CcLiveRelay.Config;
using CcLiveRelay.Services;
using CcLiveRelay.Session;
using CcLiveRelay.State;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace CcLiveRelay.Tests;

/// <summary>
/// Unit tests for the relay's batched livestream-state reporting to GO services â€” the port
/// of tests/test_observer_report.py. The outbound POST is stubbed via FakeGoHandler so
/// nothing touches the network.
/// </summary>
public class ObserverReportTests
{
    private const string GoUrl = "http://go/observers";

    private static RelayOptions Options(double changeTimeout = 0.1) => new()
    {
        GoObserversUrl = GoUrl,
        GoApiKey = "relay-key",
        ObserverChangeTimeout = (int)Math.Ceiling(changeTimeout),
    };

    private sealed class Fixture
    {
        public FakeGoHandler Handler { get; } = new();
        public RelayStore Store { get; }
        public GoReporter Reporter { get; }

        public Fixture(RelayOptions? options = null)
        {
            options ??= Options();
            Store = new RelayStore(options);
            Reporter = new GoReporter(
                options, Store,
                new HttpClient(Handler) { Timeout = TimeSpan.FromSeconds(5) },
                NullLogger<GoReporter>.Instance,
                CancellationToken.None);
        }

        /// <summary>Register a session GO would consider live (header received).</summary>
        public async Task<GameSession> LiveSessionAsync(string lobbyId)
        {
            var session = Store.GetOrCreateSession(lobbyId);
            // Set the header directly, exactly as test_observer_report.py's live_session does â€”
            // apply_header would fire the immediate "stream live" report, which these batching
            // tests must control themselves.
            session.SetHeaderReceivedForTest(new byte[16]);
            return await Task.FromResult(session);
        }

        /// <summary>Wait until the reporter has posted, or fail after a deadline.</summary>
        public async Task WaitForPostsAsync(int minPosts = 1, int timeoutMs = 5000)
        {
            var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
            while (Handler.RequestBodies.Count < minPosts)
            {
                if (DateTime.UtcNow > deadline)
                    throw new Xunit.Sdk.XunitException(
                        $"timed out waiting for {minPosts} POST(s); got {Handler.RequestBodies.Count}");
                await Task.Delay(20);
            }
        }

        public List<Dictionary<string, JsonElement>> Entries()
        {
            var result = new List<Dictionary<string, JsonElement>>();
            foreach (var arr in Handler.EntryArrays())
            {
                foreach (var entry in arr.EnumerateArray())
                {
                    var dict = new Dictionary<string, JsonElement>();
                    foreach (var prop in entry.EnumerateObject())
                        dict[prop.Name] = prop.Value.Clone();
                    result.Add(dict);
                }
            }
            return result;
        }
    }

    [Fact]
    public async Task BatchCoalesces_MultipleLobbies()
    {
        var fx = new Fixture();
        var s1 = await fx.LiveSessionAsync("batch_001");
        var s2 = await fx.LiveSessionAsync("batch_002");
        var ws1 = new FakeSocket();
        var ws2 = new FakeSocket();
        var ws3 = new FakeSocket();

        s1.AddObserver(ws1, priority: false);
        s1.AddObserver(ws2, priority: false);
        s2.AddObserver(ws3, priority: false);
        await s1.RemoveObserverAsync(ws1);
        // All within the window: one shared timer, one batch.

        await fx.WaitForPostsAsync();

        var entries = fx.Entries();
        Assert.Equal(2, entries.Count);
        Assert.All(entries, e => Assert.Equal(JsonValueKind.True, e["is_live"].ValueKind));
        var byLobby = entries.ToDictionary(e => e["lobby_id"].GetString()!);
        Assert.Equal(1, byLobby["batch_001"]["observer_count"].GetInt32());
        Assert.Equal(1, byLobby["batch_002"]["observer_count"].GetInt32());
    }

    [Fact]
    public async Task TimerNotReset_ByMidWindowChange()
    {
        var fx = new Fixture(Options(changeTimeout: 1));
        var session = await fx.LiveSessionAsync("batch_mid");
        var ws1 = new FakeSocket();
        var ws2 = new FakeSocket();

        session.AddObserver(ws1, priority: false);   // arms the timer at t=0
        await Task.Delay(600);                                  // > half the window
        session.AddObserver(ws2, priority: false);   // must NOT reset the timer
        await fx.WaitForPostsAsync(1, 5000);                    // fires on the original schedule

        Assert.Single(fx.Handler.RequestBodies);
        var entries = fx.Entries();
        Assert.Single(entries);
        Assert.Equal(2, entries[0]["observer_count"].GetInt32());
    }

    [Fact]
    public async Task UnchangedCount_Skipped()
    {
        var fx = new Fixture();
        var session = await fx.LiveSessionAsync("batch_skip");
        var ws1 = new FakeSocket();
        var ws2 = new FakeSocket();

        session.AddObserver(ws1, priority: false);
        session.AddObserver(ws2, priority: false);
        await session.RemoveObserverAsync(ws1);
        await fx.WaitForPostsAsync();
        Assert.Equal(1, fx.Entries()[0]["observer_count"].GetInt32());

        // Add+remove returns the count to 1 (== last reported) -> nothing new to post.
        session.AddObserver(ws1, priority: false);
        await session.RemoveObserverAsync(ws1);
        await Task.Delay(500);
        Assert.Single(fx.Handler.RequestBodies);
    }

    [Fact]
    public async Task EndedStream_SendsIsLiveFalse()
    {
        var fx = new Fixture();
        var session = await fx.LiveSessionAsync("batch_ended");
        var ws1 = new FakeSocket();
        session.AddObserver(ws1, priority: false);   // count change
        fx.Reporter.MarkStreamEnded("batch_ended");             // stream closes before the flush

        await fx.WaitForPostsAsync();

        var entries = fx.Entries();
        Assert.Single(entries);
        Assert.Equal(JsonValueKind.False, entries[0]["is_live"].ValueKind);
        Assert.Equal(0, entries[0]["observer_count"].GetInt32());
    }

    [Fact]
    public async Task StreamGoingLive_BypassesTheBatch()
    {
        var fx = new Fixture(Options(changeTimeout: 30));   // far longer than this test waits
        await fx.LiveSessionAsync("batch_live");

        await fx.Reporter.ReportStreamLiveAsync("batch_live");

        // No wait: if this went through the shared timer there would be nothing here yet.
        var entries = fx.Entries();
        Assert.Single(entries);
        Assert.Equal("batch_live", entries[0]["lobby_id"].GetString());
        Assert.Equal(0, entries[0]["observer_count"].GetInt32());
        Assert.Equal(JsonValueKind.True, entries[0]["is_live"].ValueKind);

        // A session with no header is not watchable, so it is not live either.
        var noHeader = fx.Store.GetOrCreateSession("batch_live_noheader");
        Assert.NotNull(noHeader);
        await fx.Reporter.ReportStreamLiveAsync("batch_live_noheader");
        Assert.Single(fx.Handler.RequestBodies);
    }

    [Fact]
    public async Task FailedLiveReport_FallsBackToTheBatch()
    {
        var fx = new Fixture();
        fx.Handler.Statuses.Enqueue(System.Net.HttpStatusCode.InternalServerError);
        await fx.LiveSessionAsync("batch_live_retry");

        await fx.Reporter.ReportStreamLiveAsync("batch_live_retry");
        Assert.Single(fx.Handler.RequestBodies);

        await fx.WaitForPostsAsync(2);
        var entries = fx.Entries();
        Assert.Equal(2, entries.Count);
        Assert.Equal("batch_live_retry", entries[1]["lobby_id"].GetString());
        Assert.Equal(JsonValueKind.True, entries[1]["is_live"].ValueKind);
    }

    [Fact]
    public async Task NoUrl_MeansNoPosts()
    {
        var fx = new Fixture(new RelayOptions());   // GO_OBSERVERS_URL empty
        var session = await fx.LiveSessionAsync("batch_off");
        var ws = new FakeSocket();
        session.AddObserver(ws, priority: false);
        await fx.Reporter.ReportStreamLiveAsync("batch_off");
        await Task.Delay(200);
        Assert.Empty(fx.Handler.RequestBodies);
    }
}
