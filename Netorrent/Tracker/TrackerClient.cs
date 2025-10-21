using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using Netorrent.Bencoding;
using Netorrent.Bencoding.Structs;
using Netorrent.IO;
using Netorrent.P2P;
using Netorrent.TorrentFile.FileStructure;
using TimeSpanXt;

namespace Netorrent.Tracker;

internal class TrackerClient(
    P2PClient p2PClient,
    HttpClient client,
    PeerIdService peerIdService,
    byte[] infoHash,
    string announceUrl,
    IFilesHandler filesHandler
) : IAsyncDisposable
{
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        var httpTrackerResponse = await Announce(Events.Started, cancellationToken);

        //TODO Handle the response connect to peers
        foreach (var item in httpTrackerResponse.Peers)
        {
            await p2PClient.ConnectToPeerAsync(item, cancellationToken);
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(httpTrackerResponse.Interval.Seconds(), cancellationToken);
            // Here you would typically send another request to the tracker to update status

            var response = await Announce(cancellationToken: cancellationToken);
        }
    }

    internal async Task<HttpTrackerResponse> Announce(
        string? @event = null,
        CancellationToken cancellationToken = default
    )
    {
        var request = new HttpTrackerRequest(
            infoHash,
            peerIdService.PeerId,
            p2PClient.Port,
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
    }
}
