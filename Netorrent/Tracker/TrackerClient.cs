using Microsoft.Extensions.Logging;
using Netorrent.P2P;
using TimeSpanXt;

namespace Netorrent.Tracker;

internal class TrackerClient(
    P2PClient p2PClient,
    HttpClient client,
    string peerId,
    byte[] infoHash,
    string announceUrl,
    ILogger logger
) : IAsyncDisposable
{
    private Task? _trackerTask;
    private readonly ILogger _logger = logger;

    public Task TrackerTask => _trackerTask ?? Task.CompletedTask;

    public void Start(CancellationToken cancellationToken = default) =>
        _trackerTask ??= Task.Run(
            async () =>
            {
                _logger.LogInformation(Convert.ToHexString(infoHash).ToLower());
                var httpTrackerResponse = await Announce(Events.Started, cancellationToken);

                await ConnectToPeers(httpTrackerResponse, cancellationToken);

                while (!cancellationToken.IsCancellationRequested)
                {
                    var interval = httpTrackerResponse.Interval.Seconds();
                    _logger.LogInformation("Waiting {seconds} seconds", interval.TotalSeconds);

                    await Task.Delay(interval, cancellationToken);

                    var response = await Announce(cancellationToken: cancellationToken);
                    await ConnectToPeers(response, cancellationToken);
                }
            },
            cancellationToken
        );

    private async Task ConnectToPeers(
        HttpTrackerResponse httpTrackerResponse,
        CancellationToken cancellationToken
    )
    {
        var connectTasks = httpTrackerResponse.Peers.Select(async item =>
            await p2PClient.ConnectToPeerAsync(item, cancellationToken)
        );

        await Task.WhenAll(connectTasks);
    }

    private async Task<HttpTrackerResponse> Announce(
        string? @event = null,
        CancellationToken cancellationToken = default
    )
    {
        _logger.LogInformation("Announcing to {url}", announceUrl);

        var request = new HttpTrackerRequest(
            infoHash,
            peerId,
            p2PClient.EndPoint.Port,
            p2PClient.FileManager.GetWrittenBytes(),
            0, //TODO Implement this in p2pclient
            p2PClient.FileManager.GetMissingBytes(),
            true,
            false,
            @event,
            NumWant: 50
        );

        var response = await client.SendAsync(
            request.GenerateRequest(announceUrl),
            cancellationToken
        );
        var httpTrackerResponse = await HttpTrackerResponse.FromHttpResponseAsync(
            response,
            cancellationToken
        );
        return httpTrackerResponse;
    }

    public async ValueTask DisposeAsync()
    {
        await Announce(Events.Stopped);
        await p2PClient.DisposeAsync();
    }
}
