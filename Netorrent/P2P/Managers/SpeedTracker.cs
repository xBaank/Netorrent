using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Netorrent.P2P.Managers;

public class SpeedTracker
{
    private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
    private long _bytesSinceLast;
    private TimeSpan _lastTime;

    public double CurrentBps { get; private set; }
    public long TotalBytes { get; private set; }

    public void AddBytes(int count)
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
