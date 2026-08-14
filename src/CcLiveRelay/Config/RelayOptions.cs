namespace CcLiveRelay.Config;

/// <summary>
/// Strongly-typed relay configuration, read once from environment variables with
/// defaults.
/// </summary>
public sealed class RelayOptions
{
    public string Host { get; init; } = "0.0.0.0";
    public int Port { get; init; } = 8765;
    public int MaxObserversPerGame { get; init; } = 1000;
    public int InactiveGameTtl { get; init; } = 60;
    public int UndescribedGameTtl { get; init; } = 120;
    public int DefaultDelaySeconds { get; init; } = 15;
    public int MaxDelaySeconds { get; init; } = 600;
    public double DelayFlushInterval { get; init; } = 1.0;
    public int BodyHistoryMax { get; init; } = 50_000;
    /// <summary>Frames buffered per observer before it is dropped as unable to keep up
    /// (rejoin re-serves catch-up). Live BODY frames are ~4 KB, catch-up chunks ChunkSize.</summary>
    public int ObserverQueueFrames { get; init; } = 1024;
    public int SourceLagBytes { get; init; } = 64 * 1024;
    public int SourceGapStrikes { get; init; } = 3;
    public int SourceSilenceSeconds { get; init; } = 10;
    public bool Debug { get; init; } = false;
    public string InternalApiKey { get; init; } = "";
    public int WatchTicketTtlSeconds { get; init; } = 30;
    public string PublicHost { get; init; } = "";
    public string PublicWsScheme { get; init; } = "wss";
    public string PublicPathPrefix { get; init; } = "";
    public string GoApiKey { get; init; } = "";
    public string GoObserversUrl { get; init; } = "";
    public int ObserverUpdateInterval { get; init; } = 60;
    public int ObserverChangeTimeout { get; init; } = 15;
    public int ChatHistoryMax { get; init; } = 200;
    public int ChatCatchupCount { get; init; } = 10;
    public int SpectatorRateMax { get; init; } = 5;
    public int SpectatorRateWindow { get; init; } = 10;
    public int ChunkSize { get; init; } = 256 * 1024;

    public static RelayOptions FromEnvironment() => new()
    {
        Host = Env("HOST", "0.0.0.0"),
        Port = EnvInt("PORT", 8765),
        MaxObserversPerGame = EnvInt("MAX_OBSERVERS_PER_GAME", 1000),
        UndescribedGameTtl = EnvInt("UNDESCRIBED_GAME_TTL", 120),
        DefaultDelaySeconds = EnvInt("DEFAULT_DELAY_SECONDS", 15),
        DelayFlushInterval = EnvDouble("DELAY_FLUSH_INTERVAL", 1.0),
        ObserverQueueFrames = EnvInt("OBSERVER_QUEUE_FRAMES", 1024),
        SourceLagBytes = EnvInt("SOURCE_LAG_BYTES", 64 * 1024),
        SourceGapStrikes = EnvInt("SOURCE_GAP_STRIKES", 3),
        SourceSilenceSeconds = EnvInt("SOURCE_SILENCE_SECONDS", 10),
        Debug = Env("DEBUG", "").Trim().ToLowerInvariant() is "1" or "true" or "yes" or "on",
        InternalApiKey = Env("INTERNAL_API_KEY", ""),
        WatchTicketTtlSeconds = EnvInt("WATCH_TICKET_TTL_SECONDS", 30),
        PublicHost = Env("PUBLIC_HOST", ""),
        PublicWsScheme = Env("PUBLIC_WS_SCHEME", "wss"),
        PublicPathPrefix = Env("PUBLIC_PATH_PREFIX", ""),
        GoApiKey = Env("GO_API_KEY", ""),
        GoObserversUrl = Env("GO_OBSERVERS_URL", ""),
        ObserverUpdateInterval = EnvInt("OBSERVER_UPDATE_INTERVAL", 60),
        ObserverChangeTimeout = EnvInt("OBSERVER_CHANGE_TIMEOUT", 15),
    };

    private static string Env(string name, string fallback) =>
        Environment.GetEnvironmentVariable(name) ?? fallback;

    private static int EnvInt(string name, int fallback) =>
        int.TryParse(Environment.GetEnvironmentVariable(name), out var v) ? v : fallback;

    private static double EnvDouble(string name, double fallback) =>
        double.TryParse(Environment.GetEnvironmentVariable(name),
                        System.Globalization.NumberStyles.Float,
                        System.Globalization.CultureInfo.InvariantCulture, out var v) ? v : fallback;
}
