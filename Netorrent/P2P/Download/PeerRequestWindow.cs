namespace Netorrent.P2P.Download;

internal class PeerRequestWindow(int blockSize)
{
    private const int MinRequests = 4;
    private const int MaxRequests = 64;
    private const double SafetyFactor = 1.2;

    public int BlockSize { get; } = blockSize;
    private double _smoothedRttSeconds = 0.2;
    private readonly Lock _windowLock = new();

    private int _maxInFlightRequests = MinRequests;

    public int MaxInFlightRequests => Volatile.Read(ref _maxInFlightRequests);

    //TODO Split into rtt calculate method and calculate window. rtt method should be called each request block arrives and calculate window each 500ms with speed tracker
    public void CalculateWindow(long bytesPerSecond, TimeSpan rtt)
    {
        lock (_windowLock)
        {
            _smoothedRttSeconds = 0.875 * _smoothedRttSeconds + 0.125 * rtt.TotalSeconds;

            if (bytesPerSecond <= 0 || _smoothedRttSeconds <= 0)
                _maxInFlightRequests = MinRequests;

            double bdp = bytesPerSecond * _smoothedRttSeconds;
            double neededBlocks = bdp / BlockSize;

            neededBlocks *= SafetyFactor;

            int window = (int)Math.Ceiling(neededBlocks);

            _maxInFlightRequests = Math.Clamp(window, MinRequests, MaxRequests);
        }
    }
}
