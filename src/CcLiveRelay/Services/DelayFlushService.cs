using CcLiveRelay.Config;
using CcLiveRelay.State;

namespace CcLiveRelay.Services;

/// <summary>
/// Deliver held observers' due body bytes on a fixed cadence. Flush-on-append covers the
/// common case; this catches chunks whose delay elapsed while no body arrived nearby (quiet
/// moments, appends stalled). Cheap when idle: no held observers, no work.
/// </summary>
public sealed class DelayFlushService : BackgroundService
{
    private readonly RelayOptions _options;
    private readonly RelayStore _store;
    private readonly ILogger<DelayFlushService> _log;

    public DelayFlushService(RelayOptions options, RelayStore store, ILogger<DelayFlushService> log)
    {
        _options = options;
        _store = store;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(_options.DelayFlushInterval));
        while (await timer.WaitForNextTickAsync(ct))
        {
            foreach (var session in _store.Games.Values.ToList())
            {
                if (session.Ended)
                    continue;
                try
                {
                    session.FlushHeldObservers();
                }
                catch (Exception e)
                {
                    _log.LogWarning(e, "[OBSERVER] [WARN] delay flush failed for game {LobbyId}",
                        session.LobbyId);
                }
            }
        }
    }
}
