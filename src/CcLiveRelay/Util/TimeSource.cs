namespace CcLiveRelay.Util;

/// <summary>
/// Clock seam for fractional unix seconds. The default reads the real clock; unit tests
/// swap in a FakeClock so arrival stamps, watermarks and flush deadlines are deterministic.
/// Test-only in practice; never read this field outside that intent.
/// </summary>
public static class TimeSource
{
    public static Func<double> Now =
        static () => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0;
}
