using System.Net;
using System.Threading.Channels;
using Netorrent.ActorSystem;
using Netorrent.Exceptions;
using Netorrent.Extensions;
using Netorrent.P2P.Messages;
using Netorrent.Statistics;
using Netorrent.TorrentFile.FileStructure;
using R3;

namespace Netorrent.Tracker.Http;

internal class HttpTracker(
    Bitfield myBitfield,
    int port,
    DataStatistics transfer,
    IHttpTrackerHandler httpTrackerHandler,
    PeerId peerId,
    InfoHash infoHash,
    string announceUrl,
    ChannelWriter<IPEndPoint> channelWriter
) : ITracker
{
    private readonly Actor<TrackerMessage> _actor = new();
    private IDisposable? _completedSubscription;
    private static readonly TrackerMessage.CompletedMessage _completedMessage = new();

    public async ValueTask StartAsync(CancellationToken cancellationToken)
    {
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
            _actor.CancelScheduled();

        var response = await AnnounceAsync(@event, cancellationToken).ConfigureAwait(false);

        foreach (var endpoint in response.Peers)
            await channelWriter.WriteAsync(endpoint, cancellationToken).ConfigureAwait(false);

        if (message is not TrackerMessage.CompletedMessage)
            _actor.ScheduleOnce(response.Interval.Seconds, new TrackerMessage.AnnounceMessage(null));
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
        await AnnounceAsync(Events.Stopped, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        _completedSubscription?.Dispose();
        await _actor.DisposeAsync().ConfigureAwait(false);
    }
}
