using System.Net.Sockets;

namespace Netorrent.Tracker.Http;

internal interface IHttpTrackerHandler
{
    ValueTask<HttpTrackerResponse> SendAsync(
        string url,
        AddressFamily addressFamily,
        HttpTrackerRequest httpTrackerRequest,
        CancellationToken cancellationToken
    );
}
