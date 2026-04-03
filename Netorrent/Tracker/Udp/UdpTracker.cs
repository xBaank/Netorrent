using System.Net;
using System.Threading.Channels;
using Netorrent.ActorSystem;
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
    private readonly Actor<TrackerMessage> _actor = new();
    private Timer? _announceTimer;
    private IDisposable? _completedSubscription;
    private static readonly TrackerMessage.CompletedMessage _completedMessage = new();

    public async ValueTask StartAsync(CancellationToken cancellationToken)
    {
        await ConnectAsync(iPEndPoint, cancellationToken).ConfigureAwait(false);

        var initialEvent = myBitfield.IsComplete ? Events.Completed : Events.Started;
        _actor.Tell(new TrackerMessage.AnnounceMessage(initialEvent));

        _completedSubscription = myBitfield.StateChanged.Subscribe(_ =>
        {
            if (myBitfield.IsComplete)
                _actor.Tell(_completedMessage);
        });

        await _actor.StartAsync(OnReceiveAsync, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask OnReceiveAsync(
        TrackerMessage message,
        CancellationToken cancellationToken
    )
    {
        string? @event = message switch
        {
            TrackerMessage.AnnounceMessage m => m.Event,
            TrackerMessage.CompletedMessage => Events.Completed,
            _ => null,
        };

        if (message is TrackerMessage.CompletedMessage)
            _announceTimer?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

        _lastResponse = await AnnounceAndReceiveAsync(iPEndPoint, @event, cancellationToken)
            .ConfigureAwait(false);

        foreach (var peer in _lastResponse.Peers)
            await channelWriter.WriteAsync(peer, cancellationToken).ConfigureAwait(false);

        ScheduleNextAnnounce(_lastResponse.Interval.Seconds);
    }

    private void ScheduleNextAnnounce(TimeSpan delay)
    {
        if (_announceTimer is null)
            _announceTimer = new Timer(
                _ => _actor.Tell(new TrackerMessage.AnnounceMessage(null)),
                null,
                delay,
                Timeout.InfiniteTimeSpan
            );
        else
            _announceTimer.Change(delay, Timeout.InfiniteTimeSpan);
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

    public async ValueTask DisposeAsync()
    {
        _completedSubscription?.Dispose();

        if (_announceTimer is not null)
        {
            await _announceTimer.DisposeAsync().ConfigureAwait(false);
            _announceTimer = null;
        }

        await _actor.DisposeAsync().ConfigureAwait(false);

        await foreach (var _ in _actor.MailboxReader.ReadAllAsync().ConfigureAwait(false)) { }
    }
}
