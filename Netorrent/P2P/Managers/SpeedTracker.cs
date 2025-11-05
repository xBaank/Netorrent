using System.Diagnostics;

namespace Netorrent.P2P.Managers;

public readonly struct DownloadSpeed(double bps)
{
    public double Bps => bps;
    public double Kbps => bps / 1_000d;
    public double Mbps => bps / 1_000_000d;
    public double Gbps => bps / 1_000_000_000d;

    public static implicit operator DownloadSpeed(double bps) => new(bps);

    public static DownloadSpeed operator +(DownloadSpeed left, DownloadSpeed right) =>
        new(left.Bps + right.Bps);

    public static DownloadSpeed operator -(DownloadSpeed left, DownloadSpeed right) =>
        new(left.Bps - right.Bps);

    public override string ToString()
    {
        if (bps >= 1_000_000_000)
            return $"{Gbps:F2}/Gbps";
        if (bps >= 1_000_000)
            return $"{Mbps:F2}/Mbps";
        if (bps >= 1_000)
            return $"{Kbps:F2}/Kbps";
        return $"{bps:F2}/bps";
    }
}

public class SpeedTracker
{
    private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
    private readonly Lock _lock = new();
    private long _bytesSinceLast;
    private TimeSpan _lastTime;

    public DownloadSpeed CurrentBps { get; private set; }
    public long TotalBytes { get; private set; }

    public void AddBytes(int count)
    {
        lock (_lock)
        {
            TotalBytes += count;
            _bytesSinceLast += count;

            var now = _stopwatch.Elapsed;
            var delta = now - _lastTime;

            if (delta.TotalSeconds >= 1)
            {
                CurrentBps = _bytesSinceLast / delta.TotalSeconds;
                _bytesSinceLast = 0;
                _lastTime = now;
            }
        }
    }
}
