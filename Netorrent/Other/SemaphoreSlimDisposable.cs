namespace Netorrent.Other;

public readonly struct SemaphoreSlimDisposable(SemaphoreSlim semaphoreSlim) : IDisposable
{
    public void Dispose() => semaphoreSlim.Release();
}
