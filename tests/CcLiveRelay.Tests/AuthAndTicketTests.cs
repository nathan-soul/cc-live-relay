using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using CcLiveRelay.Protocol;
using Xunit;

namespace CcLiveRelay.Tests;

/// <summary>
/// GO-orchestrated ticket flow over the real HTTP surface — the port of
/// tests/test_relay_auth_mock.py (in-process TestServer, no real sockets).
/// </summary>
public class AuthAndTicketTests : IClassFixture<AuthAndTicketTests.App>
{
    public sealed class App : Microsoft.AspNetCore.Mvc.Testing.WebApplicationFactory<Program>
    {
        static App() => TestEnv.Configure();
    }

    private readonly App _app;

    public AuthAndTicketTests(App app) => _app = app;

    private HttpClient Keys(HttpClient client)
    {
        client.DefaultRequestHeaders.Add("X-Relay-Key", "test123");
        return client;
    }

    private async Task<(string BaseUrl, string LobbyId)> CreateLivestreamAsync(long ownerUserId = 1,
                                                                                int? delaySeconds = null)
    {
        var client = Keys(_app.CreateClient());
        var payload = delaySeconds is null
            ? $"{{\"lobby_id\":\"auth_{Guid.NewGuid():N}\",\"owner_user_id\":{ownerUserId}}}"
            : $"{{\"lobby_id\":\"auth_{Guid.NewGuid():N}\",\"owner_user_id\":{ownerUserId}," +
              $"\"delay_seconds\":{delaySeconds}}}";
        var response = await client.PostAsync("/internal/livestreams",
            new StringContent(payload, Encoding.UTF8, "application/json"));
        Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        var baseUrl = doc.RootElement.GetProperty("base_url").GetString()!;
        var lobbyId = baseUrl[(baseUrl.LastIndexOf('/') + 1)..];
        return (baseUrl, lobbyId);
    }

    private async Task<string> MintWatchTicketAsync(string lobbyId, long userId, bool priority = false)
    {
        var client = Keys(_app.CreateClient());
        var response = await client.PostAsync("/internal/watch_tickets",
            new StringContent($"{{\"lobby_id\":\"{lobbyId}\",\"user_id\":{userId}," +
                              $"\"priority\":{(priority ? "true" : "false")}}}",
                Encoding.UTF8, "application/json"));
        Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        return doc.RootElement.GetProperty("url").GetString()!;
    }

    [Fact]
    public async Task Health_ReportsOk()
    {
        var client = _app.CreateClient();
        var response = await client.GetAsync("/health");
        Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("ok", doc.RootElement.GetProperty("status").GetString());
        Assert.True(doc.RootElement.TryGetProperty("active_games", out _));
        Assert.True(doc.RootElement.TryGetProperty("total_observers", out _));
        Assert.True(doc.RootElement.TryGetProperty("total_body_bytes", out _));
    }

    [Fact]
    public async Task InternalEndpoints_RequireKey()
    {
        var client = _app.CreateClient();
        var response = await client.PostAsync("/internal/livestreams",
            new StringContent("{\"lobby_id\":\"auth_nokey\"}", Encoding.UTF8, "application/json"));
        Assert.Equal(System.Net.HttpStatusCode.Unauthorized, response.StatusCode);
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Contains("relay key", doc.RootElement.GetProperty("detail").GetString());
    }

    [Fact]
    public async Task Livestream_RegistersDelay()
    {
        var (_, lobbyId) = await CreateLivestreamAsync(delaySeconds: 5);
        var session = _app.Services.GetRequiredService<CcLiveRelay.State.RelayStore>()
            .Games[lobbyId];
        Assert.Equal(5, session.DelaySeconds);
        Assert.True(session.DelayFromGo);
    }

    [Fact]
    public async Task WatchTicket_UnknownLobby_ReturnsStreamEnded404()
    {
        var client = Keys(_app.CreateClient());
        var response = await client.PostAsync("/internal/watch_tickets",
            new StringContent("{\"lobby_id\":\"no_such_lobby\",\"user_id\":1}",
                Encoding.UTF8, "application/json"));
        Assert.Equal(System.Net.HttpStatusCode.NotFound, response.StatusCode);
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        var detail = doc.RootElement.GetProperty("detail");
        Assert.Equal("stream_ended", detail.GetProperty("code").GetString());
    }

    [Fact]
    public async Task WatchTicket_MintCarriesPriorityAndTtl()
    {
        var (_, lobbyId) = await CreateLivestreamAsync();
        string url = await MintWatchTicketAsync(lobbyId, userId: 9, priority: true);
        Assert.StartsWith($"wss://", url);
        Assert.Contains($"/watch/{lobbyId}?ticket=", url);

        var store = _app.Services.GetRequiredService<CcLiveRelay.State.RelayStore>();
        var key = url[(url.LastIndexOf('=') + 1)..];
        var cred = store.WatchTickets[key];
        Assert.NotNull(cred);
        Assert.True(cred.Priority);
        Assert.Equal(9, cred.UserId);
        Assert.InRange(cred.ExpiresAt, 
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0 + 20,
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0 + 40);
    }

    [Fact]
    public async Task StreamToken_Mint()
    {
        var (_, lobbyId) = await CreateLivestreamAsync();
        var client = Keys(_app.CreateClient());
        var response = await client.PostAsync("/internal/stream_tokens",
            new StringContent($"{{\"lobby_id\":\"{lobbyId}\",\"user_id\":1}}",
                Encoding.UTF8, "application/json"));
        Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        var url = doc.RootElement.GetProperty("url").GetString()!;
        Assert.Contains($"/stream/{lobbyId}?stream_token=", url);
    }
}
