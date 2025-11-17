using System.Diagnostics;

namespace Netorrent.P2P.Measurement;

public class SpeedTracker
{
    private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
    private readonly Lock _lock = new();
    private long _bytesSinceLast;
    private TimeSpan _lastTime;

    public DownloadSpeed CurrentBps => GetCurrentBps();
    public ByteSize TotalBytes { get; private set; }

    public void AddBytes(int count)
    {
        lock (_lock)
        {
            TotalBytes += count;
            _bytesSinceLast += count;
        }
    }

    private DownloadSpeed GetCurrentBps()
    {
        var now = _stopwatch.Elapsed;
        var delta = now - _lastTime;
        var toReturn = _bytesSinceLast / delta.TotalSeconds;
        _bytesSinceLast = 0;
        _lastTime = now;
        return toReturn;
    }
}
