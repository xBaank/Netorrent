using Netorrent.P2P.Measurement;

namespace Netorrent.P2P.Download;

internal class PeerRequestWindow(int blockSize)
{
    private const double SafetyFactor = 1.5;
    private const ulong MinRequests = 4;

    private ulong _maxInFlightRequests = MinRequests;
    private ulong _lastbytesPerSecond = 0;
    private bool _slowStart = true;

    public ulong MaxInFlightRequests => Volatile.Read(ref _maxInFlightRequests);

    private void CalculateWindow(ulong bytesPerSecond)
    {
        if (_slowStart)
        {
            return;
        }

        var neededBlocks = bytesPerSecond / (uint)blockSize;
        _maxInFlightRequests = (ulong)(Math.Max(neededBlocks, MinRequests) * SafetyFactor);
    }

    public void ReceivedBlock(ulong bytesPerSecond)
    {
        if (!_slowStart)
        {
            return;
        }

        if (bytesPerSecond < _lastbytesPerSecond)
        {
            _slowStart = false;
        }

        if (_slowStart)
        {
            _lastbytesPerSecond = bytesPerSecond;
            _maxInFlightRequests += 1;
        }
    }

    public Timer StartSampling(TimeSpan period, SpeedTracker speedTracker)
    {
        return new Timer(
            _ => CalculateWindow((ulong)speedTracker.Speed.Bps),
            null,
            TimeSpan.Zero,
            period
        );
    }
}
