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

        _lastResponse = await AnnounceAndReceiveAsync(
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
                    _lastResponse = await AnnounceAndReceiveAsync(
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

            var newResponse = await AnnounceAndReceiveAsync(iPEndPoint, null, cancellationToken)
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

    public async Task<UdpTrackerResponse> AnnounceAndReceiveAsync(
        IPEndPoint iPEndPoint,
        string? @event,
        CancellationToken cancellationToken
    )
    {
        var (_, request) = await BuildRequestAsync(iPEndPoint, @event, cancellationToken)
            .ConfigureAwait(false);

        try
        {
            return await udpTrackerHandler
                .SendAndReceiveAsync<UdpTrackerResponse>(request, _trackerId, cancellationToken)
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

    public async Task AnnounceAsync(
        IPEndPoint iPEndPoint,
        string? @event,
        CancellationToken cancellationToken
    )
    {
        var (_, request) = await BuildRequestAsync(iPEndPoint, @event, cancellationToken)
            .ConfigureAwait(false);

        try
        {
            await udpTrackerHandler
                .SendAsync(request, _trackerId, cancellationToken)
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

    private async Task<(long ConnectionId, UdpTrackerRequest Request)> BuildRequestAsync(
        IPEndPoint iPEndPoint,
        string? @event,
        CancellationToken cancellationToken
    )
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

        return (connectionId.Value, updRequest);
    }

    public async ValueTask StopAsync(CancellationToken cancellationToken)
    {
        if (iPEndPoint is not null && _lastResponse is not null)
        {
            await AnnounceAsync(iPEndPoint, Events.Stopped, cancellationToken)
                .ConfigureAwait(false);
        }
    }
}
