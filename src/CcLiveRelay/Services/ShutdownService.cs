using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using CcLiveRelay.Config;
using CcLiveRelay.Protocol;
using CcLiveRelay.State;

using CcLiveRelay.Session;
namespace CcLiveRelay.Services;

/// <summary>
/// The relay is going down (deploy/restart/stop). Every healthy session is being torn out
/// from under its clients, so tell them: sources get an ERROR frame and their sockets close
/// (a graceful game end never looks like this), observers get the same ERROR so they can
/// show "stream lost" in-game instead of waiting out their own watchdog on a dead socket.
    /// Never raises — shutdown must not fail.
/// </summary>
public sealed class ShutdownService : IHostedService
{
    private readonly RelayOptions _options;
    private readonly RelayStore _store;
    private readonly ILogger<ShutdownService> _log;

    public ShutdownService(RelayOptions options, RelayStore store, ILogger<ShutdownService> log)
    {
        _options = options;
        _store = store;
        _log = log;
    }

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        int liveSessions = 0;
        foreach (var session in _store.Games.Values.ToList())
        {
            if (session.Ended)
                continue;
            liveSessions++;
            try
            {
                var reasonJson = JsonSerializer.Serialize(new
                {
                    reason = "relay-shutdown",
                    msg = "relay is going down",
                }, GameSession.JsonOpts);
                session.BroadcastEnvelope(BinaryEnvelope.MsgError,
                    Encoding.UTF8.GetBytes(reasonJson));
            }
            catch (Exception)
            {
                // ignore — shutdown must not fail
            }
            foreach (var ws in session.SourcesSnapshot)
            {
                try
                {
                    await ws.CloseAsync();
                }
                catch (Exception)
                {
                    // ignore
                }
            }
        }
        if (liveSessions > 0)
        {
            _log.LogWarning("[LIVESTREAMER] [SHUTDOWN] Relay shutting down with {Count} live session(s); " +
                            "notified their observers and sources", liveSessions);
        }
    }
}
