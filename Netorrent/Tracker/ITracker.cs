namespace Netorrent.Tracker;

internal interface ITracker : IAsyncDisposable
{
    public Task? TrackerTask { get; }
    public void Start(CancellationToken cancellationToken);
}
