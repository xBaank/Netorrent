using System.Net;
using Netorrent.Tracker.Http;

namespace Netorrent.Tests.Fakes;

internal class FakeHttpTrackerHandler(IPEndPoint[] ips, TimeSpan interval) : IHttpTrackerHandler
{
    public ValueTask<HttpTrackerResponse> SendAsync(
        string url,
        HttpTrackerRequest httpTrackerRequest,
        CancellationToken cancellationToken
    ) =>
        ValueTask.FromResult(
            new HttpTrackerResponse
            {
                Interval = (int)interval.TotalSeconds,
                Complete = ips.Length,
                Incomplete = 0,
                Peers = [.. ips],
            }
        );
}
