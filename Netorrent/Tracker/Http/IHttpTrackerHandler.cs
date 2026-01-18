using System.Net.Sockets;

namespace Netorrent.Tracker.Http;

internal interface IHttpTrackerHandler : IDisposable
{
    ValueTask<HttpTrackerResponse> SendAsync(
        string url,
        HttpTrackerRequest httpTrackerRequest,
        CancellationToken cancellationToken
    );
}
