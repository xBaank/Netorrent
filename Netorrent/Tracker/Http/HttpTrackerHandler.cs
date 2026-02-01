namespace Netorrent.Tracker.Http;

internal class HttpTrackerHandler(HttpClient httpClient) : IHttpTrackerHandler
{
    public async ValueTask<HttpTrackerResponse> SendAsync(
        string url,
        HttpTrackerRequest httpTrackerRequest,
        CancellationToken cancellationToken
    )
    {
        var response = await httpClient
            .SendAsync(httpTrackerRequest.GenerateRequest(url), cancellationToken)
            .ConfigureAwait(false);

        return await HttpTrackerResponse
            .FromHttpResponseAsync(response, cancellationToken)
            .ConfigureAwait(false);
    }

    public void Dispose()
    {
        httpClient.Dispose();
    }
}
