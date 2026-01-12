using Netorrent.P2P.Measurement;

namespace Netorrent.P2P.Download;

internal class PeerRequestWindow(int blockSize)
{
    private const double SafetyFactor = 1.5;

    private ulong _maxInFlightRequests = 4;
    private double _lastbytesPerSecond = 0;
    private bool _slowStart = true;

    public ulong MaxInFlightRequests => _maxInFlightRequests;

    private void CalculateWindow(double bytesPerSecond)
    {
        if (_slowStart)
        {
            return;
        }

        var neededBlocks = bytesPerSecond / blockSize;
        _maxInFlightRequests = (ulong)(neededBlocks * SafetyFactor);
    }

    public void ReceivedBlock(double bytesPerSecond)
    {
        if (!_slowStart)
        {
            return;
        }

        if (bytesPerSecond < _lastbytesPerSecond)
        {
            _slowStart = false;
            return;
        }

        _lastbytesPerSecond = bytesPerSecond;
        _maxInFlightRequests += 1;
    }

    public Timer StartSampling(TimeSpan period, SpeedTracker speedTracker)
    {
        return new Timer(_ => CalculateWindow(speedTracker.Speed.Bps), null, TimeSpan.Zero, period);
    }
}
