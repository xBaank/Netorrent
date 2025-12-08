namespace Netorrent.P2P.Download;

internal class PeerRequestWindow(int blockSize)
{
    private const int MinRequests = 4;
    private const int MaxRequests = 64;
    private const double SafetyFactor = 1.2;

    public int BlockSize { get; } = blockSize;
    private double _smoothedRttSeconds = 0.2;
    private readonly Lock _windowLock = new();

    private volatile int _maxInFlightRequests = MinRequests;

    public int MaxInFlightRequests
    {
        get => _maxInFlightRequests;
        private set => _maxInFlightRequests = value;
    }

    public void CalculateWindow(long bytesPerSecond, TimeSpan rtt)
    {
        lock (_windowLock)
        {
            _smoothedRttSeconds = 0.875 * _smoothedRttSeconds + 0.125 * rtt.TotalSeconds;

            if (bytesPerSecond <= 0 || _smoothedRttSeconds <= 0)
                MaxInFlightRequests = MinRequests;

            double bdp = bytesPerSecond * _smoothedRttSeconds;
            double neededBlocks = bdp / BlockSize;

            neededBlocks *= SafetyFactor;

            int window = (int)Math.Ceiling(neededBlocks);

            MaxInFlightRequests = Math.Clamp(window, MinRequests, MaxRequests);
        }
    }
}
