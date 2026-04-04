namespace Netorrent.Tracker;

internal interface ITracker : IAsyncDisposable
{
    public ValueTask StartAsync(CancellationToken cancellationToken);
    public ValueTask StopAsync(CancellationToken cancellationToken);
}
