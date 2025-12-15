namespace Netorrent.Tracker.Http;

internal interface IHttpTrackerHandler
{
    ValueTask<HttpTrackerResponse> SendAsync(
        string url,
        HttpTrackerRequest httpTrackerRequest,
        CancellationToken cancellationToken
    );
}
