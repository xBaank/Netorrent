using System.Diagnostics;

namespace Netorrent.P2P.Measurement;

/// <summary>
/// Tracks the total number of bytes processed and calculates the current download speed using an exponential moving
/// average.
/// </summary>
public sealed class SpeedTracker(double alpha = 0.3)
{
    private long _bytesSinceLast;
    private long _totalBytes;

    private double _currentBps;
    private long _lastTimestamp = Stopwatch.GetTimestamp();

    private readonly double _alpha = alpha;

    public ByteSize TotalBytes => Interlocked.Read(ref _totalBytes);
    public DownloadSpeed Speed => Volatile.Read(ref _currentBps);

    internal Timer StartSampling(TimeSpan period)
    {
        // Timer callback should be non-blocking; it calls Sample().
        return new Timer(_ => Sample(), null, TimeSpan.Zero, period);
    }

    internal void AddBytes(int count)
    {
        if (count <= 0)
            return;
        Interlocked.Add(ref _bytesSinceLast, count);
        Interlocked.Add(ref _totalBytes, count);
    }

    private void Sample()
    {
        long now = Stopwatch.GetTimestamp();
        long prev = _lastTimestamp;
        _lastTimestamp = now;

        long bytes = Interlocked.Exchange(ref _bytesSinceLast, 0);

        double elapsedSec = (double)(now - prev) / Stopwatch.Frequency;
        if (elapsedSec <= 0)
            return;

        double instant = bytes / elapsedSec;
        _currentBps = _alpha * instant + (1 - _alpha) * _currentBps;
    }
}
