using System.Net;
using System.Threading.Channels;
using Netorrent.Exceptions;
using Netorrent.Extensions;
using Netorrent.P2P.Messages;
using Netorrent.Statistics;
using Netorrent.TorrentFile.FileStructure;
using Netorrent.Tracker.Udp.Request;
using Netorrent.Tracker.Udp.Response;
using R3;

namespace Netorrent.Tracker.Udp;

internal class UdpTracker(
    Bitfield myBitfield,
    IUdpTrackerHandler udpTrackerHandler,
    int port,
    DataStatistics transfer,
    PeerId peerId,
    ChannelWriter<IPEndPoint> channelWriter,
    InfoHash infoHash,
    IPEndPoint iPEndPoint
) : ITracker
{
    private UdpTrackerResponse? _lastResponse;
    private readonly Guid _trackerId = Guid.CreateVersion7();

    public async ValueTask StartAsync(CancellationToken cancellationToken)
    {
        await ConnectAsync(iPEndPoint, cancellationToken).ConfigureAwait(false);

        _lastResponse = await AnnounceAsync(
                iPEndPoint,
                @event: myBitfield.IsComplete ? Events.Completed : Events.Started,
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);

        using var completeDisposable = myBitfield.StateChanged.SubscribeAwait(
            async (value, ct) =>
            {
                if (myBitfield.IsComplete)
                {
                    _lastResponse = await AnnounceAsync(
                            iPEndPoint,
                            @event: Events.Completed,
                            cancellationToken: cancellationToken
                        )
                        .ConfigureAwait(false);
                }
            },
            configureAwait: false
        );

        foreach (var peer in _lastResponse.Peers)
        {
            await channelWriter.WriteAsync(peer, cancellationToken).ConfigureAwait(false);
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            var interval = _lastResponse.Interval.Seconds;
            await Task.Delay(interval, cancellationToken).ConfigureAwait(false);

            var newResponse = await AnnounceAsync(iPEndPoint, null, cancellationToken)
                .ConfigureAwait(false);

            _lastResponse = newResponse;

            foreach (var peer in _lastResponse.Peers)
            {
                await channelWriter.WriteAsync(peer, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public async Task<UdpTrackerConnectResponse> ConnectAsync(
        IPEndPoint iPEndPoint,
        CancellationToken cancellationToken
    )
    {
        try
        {
            return await udpTrackerHandler
                .ConnectAsync(iPEndPoint, _trackerId, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new AnnounceException(ex.Message);
        }
    }

    public async Task<UdpTrackerResponse> AnnounceAsync(
        IPEndPoint iPEndPoint,
        string? @event,
        CancellationToken cancellationToken
    )
    {
        try
        {
            var connectionId = udpTrackerHandler.GetConnectionIdOrNull(_trackerId);

            if (connectionId is null || udpTrackerHandler.IsOutdated(connectionId.Value))
            {
                var response = await udpTrackerHandler
                    .ConnectAsync(iPEndPoint, _trackerId, cancellationToken)
                    .ConfigureAwait(false);

                connectionId = response.ConnectionId;
            }

            var updRequest = new UdpTrackerRequest(
                iPEndPoint,
                infoHash,
                peerId,
                transfer.Downloaded.Bytes,
                transfer.Uploaded.Bytes,
                transfer.Left.Bytes,
                @event,
                (ushort)port,
                ConnectionId: connectionId.Value,
                TransactionId: udpTrackerHandler.MakeTransactionId(),
                NumWant: 200
            );

            return await udpTrackerHandler
                .SendAsync<UdpTrackerResponse>(updRequest, _trackerId, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new AnnounceException(ex.Message);
        }
    }

    public async ValueTask StopAsync(CancellationToken cancellationToken)
    {
        if (iPEndPoint is not null && _lastResponse is not null)
        {
            //Udp tracker don't respond to stop so there is no point in awaiting as it will never complete
            _ = AnnounceAsync(iPEndPoint, Events.Stopped, cancellationToken);
        }
    }
}
