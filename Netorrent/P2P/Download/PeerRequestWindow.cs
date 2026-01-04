using Netorrent.P2P.Measurement;

namespace Netorrent.P2P.Download;

internal class PeerRequestWindow(int blockSize)
{
    private const int MinRequests = 4;
    private const double SafetyFactor = 1.5;

    private int _maxInFlightRequests = MinRequests;

    public int MaxInFlightRequests => Volatile.Read(ref _maxInFlightRequests);

    private void CalculateWindow(long bytesPerSecond)
    {
        var neededBlocks = (int)bytesPerSecond / blockSize;
        _maxInFlightRequests = (int)(Math.Max(neededBlocks, MinRequests) * SafetyFactor);
    }

    public Timer StartSampling(TimeSpan period, SpeedTracker speedTracker)
    {
        return new Timer(
            _ => CalculateWindow((long)speedTracker.Speed.Bps),
            null,
            TimeSpan.Zero,
            period
        );
    }
}
