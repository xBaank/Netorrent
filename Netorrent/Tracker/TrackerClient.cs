using System.Net;
using Netorrent.P2P;
using TimeSpanXt;

namespace Netorrent.Tracker;

internal class TrackerClient(
    P2PClient p2PClient,
    HttpClient client,
    string peerId,
    byte[] infoHash,
    string announceUrl
) : IAsyncDisposable
{
    private Task _listenTask = Task.CompletedTask;

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        _listenTask = Task.Run(
            async () => await p2PClient.ListenForPeersAsync(cancellationToken),
            cancellationToken
        );
        var httpTrackerResponse = await Announce(Events.Started, cancellationToken);

        await ConnectToPeers(httpTrackerResponse, cancellationToken);

        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(httpTrackerResponse.Interval.Seconds(), cancellationToken);

            var response = await Announce(cancellationToken: cancellationToken);
            await ConnectToPeers(response, cancellationToken);
        }
    }

    private async Task ConnectToPeers(
        HttpTrackerResponse httpTrackerResponse,
        CancellationToken cancellationToken
    )
    {
        var connectTasks = httpTrackerResponse
            .Peers.Where(ep =>
                !(
                    (
                        ep.Address.Equals(IPAddress.Loopback)
                        || ep.Address.Equals(IPAddress.IPv6Loopback)
                    )
                    && ep.Port == p2PClient.EndPoint.Port
                )
            )
            .Select(async item => await p2PClient.ConnectToPeerAsync(item, cancellationToken));

        await Task.WhenAll(connectTasks);
    }

    internal async Task<HttpTrackerResponse> Announce(
        string? @event = null,
        CancellationToken cancellationToken = default
    )
    {
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
        p2PClient.Dispose();
    }
}
