using System.Net;
using System.Net.Sockets;
using Netorrent.IO;
using Netorrent.P2P;
using TimeSpanXt;

namespace Netorrent.Tracker;

internal class TrackerClient(
    P2PClient p2PClient,
    HttpClient client,
    string peerId,
    byte[] infoHash,
    string announceUrl,
    IFilesHandler filesHandler
) : IAsyncDisposable
{
    private Task _listenTask = Task.CompletedTask;

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        _listenTask = p2PClient.ListenForPeersAsync(cancellationToken);
        var httpTrackerResponse = await Announce(Events.Started, cancellationToken);

        //TODO Handle the response connect to peers
        await ConnectToPeers(httpTrackerResponse, cancellationToken);

        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(httpTrackerResponse.Interval.Seconds(), cancellationToken);
            // Here you would typically send another request to the tracker to update status

            var response = await Announce(cancellationToken: cancellationToken);
            await ConnectToPeers(response, cancellationToken);
        }
    }

    private async Task ConnectToPeers(
        HttpTrackerResponse httpTrackerResponse,
        CancellationToken cancellationToken
    )
    {
        foreach (var item in httpTrackerResponse.Peers)
        {
            await p2PClient.ConnectToPeerAsync(item, cancellationToken);
        }
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
            filesHandler.GetDownloaded(),
            filesHandler.GetUploaded(),
            filesHandler.GetLeft(),
            true,
            false,
            @event
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
