using CcLiveRelay.Config;
using CcLiveRelay.Endpoints;
using CcLiveRelay.Services;
using CcLiveRelay.State;

var builder = WebApplication.CreateBuilder(args);

var options = RelayOptions.FromEnvironment();
builder.Services.AddSingleton(options);
builder.Services.AddSingleton<RelayStore>();
builder.Services.AddSingleton(_ => new HttpClient { Timeout = TimeSpan.FromSeconds(5) });
builder.Services.AddSingleton(sp =>
{
    var lifetime = sp.GetRequiredService<IHostApplicationLifetime>();
    return new GoReporter(
        sp.GetRequiredService<RelayOptions>(),
        sp.GetRequiredService<RelayStore>(),
        sp.GetRequiredService<HttpClient>(),
        sp.GetRequiredService<ILogger<GoReporter>>(),
        lifetime.ApplicationStopping);
});
builder.Services.AddHostedService<CleanupService>();
builder.Services.AddHostedService<DelayFlushService>();
builder.Services.AddHostedService<ObserverReportService>();
builder.Services.AddHostedService<ShutdownService>();
// Verbatim JSON property names: the wire contract spells its keys exactly
// (base_url, delay_seconds, lobbyid, ...) and GO's RelayClient parses them verbatim.
builder.Services.ConfigureHttpJsonOptions(o => o.SerializerOptions.PropertyNamingPolicy = null);

builder.WebHost.UseUrls($"http://{options.Host}:{options.Port}");

var app = builder.Build();

app.UseWebSockets(new WebSocketOptions
{
    // Kestrel pings idle sockets and closes dead ones for us; 30 s matches the Services
    // project's own WebSocketOptions convention.
    KeepAliveInterval = TimeSpan.FromSeconds(30),
});

InternalEndpoints.Map(app);
StreamEndpoint.Map(app);
WatchEndpoint.Map(app);

Console.WriteLine($"[START] cc-live-relay v0.7.0 (C# port) starting on {options.Host}:{options.Port}");
Console.WriteLine($"[START] Max observers: {options.MaxObserversPerGame}, " +
                  $"Chunk size: {options.ChunkSize} bytes");
if (string.IsNullOrEmpty(options.InternalApiKey))
    Console.WriteLine("[START] WARNING: INTERNAL_API_KEY is not set — /internal/* endpoints will refuse all calls");
if (string.IsNullOrEmpty(options.GoObserversUrl))
    Console.WriteLine("[START] WARNING: GO_OBSERVERS_URL is not set — GO is told nothing about livestream " +
                      "state, so no stream will ever appear in its livestream menu");
if (!string.IsNullOrEmpty(options.GoObserversUrl) && string.IsNullOrEmpty(options.GoApiKey))
    Console.WriteLine("[START] WARNING: GO_OBSERVERS_URL is set but GO_API_KEY is not — " +
                      "GO will reject the observer-count notification (401)");

app.Run();

/// <summary>Entry point for WebApplicationFactory-based tests.</summary>
public partial class Program;
