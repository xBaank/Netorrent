namespace Netorrent.Tracker;

internal interface ITracker
{
    public ValueTask StartAsync(CancellationToken cancellationToken);
    public ValueTask StopAsync(CancellationToken cancellationToken);
}
