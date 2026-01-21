using System.Net;
using System.Threading.Channels;
using Netorrent.Exceptions;
using Netorrent.Extensions;
using Netorrent.P2P.Messages;
using Netorrent.Statistics;
using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.Tracker.Http;

internal class HttpTracker(
    int port,
    DataStatistics transfer,
    IHttpTrackerHandler httpTrackerHandler,
    PeerId peerId,
    InfoHash infoHash,
    string announceUrl,
    ChannelWriter<IPEndPoint> channelWriter
) : ITracker
{
    public async ValueTask StartAsync(CancellationToken cancellationToken)
    {
        var response = await AnnounceAsync(Events.Started, cancellationToken).ConfigureAwait(false);
        foreach (var iPEndPoint in response.Peers)
        {
            await channelWriter.WriteAsync(iPEndPoint, cancellationToken).ConfigureAwait(false);
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            var interval = response.Interval.Seconds;

            await Task.Delay(interval, cancellationToken).ConfigureAwait(false);

            var newResponse = await AnnounceAsync(cancellationToken: cancellationToken)
                .ConfigureAwait(false);
            response = newResponse;

            foreach (var iPEndPoint in response.Peers)
            {
                await channelWriter.WriteAsync(iPEndPoint, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task<HttpTrackerResponse> AnnounceAsync(
        string? @event = null,
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            var request = new HttpTrackerRequest(
                infoHash,
                peerId,
                port,
                (ulong)transfer.Downloaded.Bytes,
                (ulong)transfer.Uploaded.Bytes,
                (ulong)transfer.Left.Bytes,
                true,
                false,
                @event,
                200
            );

            return await httpTrackerHandler
                .SendAsync(announceUrl, request, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new AnnounceException(ex.Message);
        }
    }

    public async ValueTask StopAsync(CancellationToken cancellationToken)
    {
        await AnnounceAsync(Events.Stopped, cancellationToken).ConfigureAwait(false);
    }
}
