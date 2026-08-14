using CcLiveRelay.Config;
using CcLiveRelay.State;

namespace CcLiveRelay.Services;

/// <summary>
/// Periodic baseline flush of the observer-count batch to GO. Even a stream whose observer
/// set has been static for a while gets a fresh state every OBSERVER_UPDATE_INTERVAL, so
/// GO's observer_count never goes stale. Drains the same dirty sets, so lobbies whose count
/// is unchanged from the last posted value are skipped — cheap.
/// </summary>
public sealed class ObserverReportService : BackgroundService
{
    private readonly RelayOptions _options;
    private readonly GoReporter _reporter;
    private readonly ILogger<ObserverReportService> _log;

    public ObserverReportService(RelayOptions options, GoReporter reporter,
                                 ILogger<ObserverReportService> log)
    {
        _options = options;
        _reporter = reporter;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(_options.ObserverUpdateInterval));
        while (await timer.WaitForNextTickAsync(ct))
        {
            try
            {
                await _reporter.FlushObserverBatchAsync();
            }
            catch (Exception e)
            {
                _log.LogWarning(e, "[LIVESTREAM] [WARN] periodic observer batch flush failed");
            }
        }
    }
}
