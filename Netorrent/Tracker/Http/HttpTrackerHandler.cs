using System.Net.Sockets;

namespace Netorrent.Tracker.Http;

internal class HttpTrackerHandler(HttpClient httpClientIpv4, HttpClient httpClientIpv6)
    : IHttpTrackerHandler
{
    public async ValueTask<HttpTrackerResponse> SendAsync(
        string url,
        AddressFamily addressFamily,
        HttpTrackerRequest httpTrackerRequest,
        CancellationToken cancellationToken
    )
    {
        var httpClient = addressFamily switch
        {
            AddressFamily.InterNetwork => httpClientIpv4,
            AddressFamily.InterNetworkV6 => httpClientIpv6,
            _ => throw new ArgumentException("Unsupported AddressFamily", nameof(addressFamily)),
        };

        var response = await httpClient
            .SendAsync(httpTrackerRequest.GenerateRequest(url), cancellationToken)
            .ConfigureAwait(false);

        return await HttpTrackerResponse
            .FromHttpResponseAsync(response, cancellationToken)
            .ConfigureAwait(false);
    }
}
