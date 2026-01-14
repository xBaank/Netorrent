using System.Net;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.P2P.Messages;
using Netorrent.Statistics;
using Netorrent.TorrentFile.FileStructure;
using Netorrent.Tracker.Udp.Request;
using Netorrent.Tracker.Udp.Response;

namespace Netorrent.Tracker.Udp;

internal class UdpTracker(
    IUdpTrackerHandler udpTrackerHandler,
    int port,
    DataStatistics transfer,
    PeerId peerId,
    ChannelWriter<IPEndPoint> channelWriter,
    InfoHash infoHash,
    string announceUrl,
    IPEndPoint iPEndPoint,
    ILogger logger,
    IPAddress? forcedIp
) : ITracker
{
    private UdpTrackerResponse? _lastResponse;
    private readonly Guid _trackerId = Guid.CreateVersion7();

    public async ValueTask StartAsync(CancellationToken cancellationToken)
    {
        if (await TryConnectAsync(iPEndPoint, cancellationToken).ConfigureAwait(false) is null)
            return;

        _lastResponse = await TryAnnounceAsync(
                iPEndPoint,
                @event: Events.Started,
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);

        if (_lastResponse is null)
            return;

        foreach (var peer in _lastResponse.Peers)
        {
            await channelWriter.WriteAsync(peer, cancellationToken).ConfigureAwait(false);
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(_lastResponse.Interval.Seconds, cancellationToken)
                .ConfigureAwait(false);

            var interval = _lastResponse.Interval.Seconds;

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("Waiting {seconds} seconds", interval.TotalSeconds);

            var newResponse = await TryAnnounceAsync(iPEndPoint, null, cancellationToken)
                .ConfigureAwait(false);

            if (newResponse is null)
                continue;

            _lastResponse = newResponse;

            foreach (var peer in _lastResponse.Peers)
            {
                await channelWriter.WriteAsync(peer, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public async Task<UdpTrackerConnectResponse?> TryConnectAsync(
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
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug(ex, "Couldn't connect to {trackerUrl}", announceUrl);

            return null;
        }
    }

    public async Task<UdpTrackerResponse?> TryAnnounceAsync(
        IPEndPoint iPEndPoint,
        string? @event,
        CancellationToken cancellationToken
    )
    {
        try
        {
            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("Announcing to {url}", announceUrl);

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
                NumWant: 50,
                IpAddress: forcedIp
            );

            return await udpTrackerHandler
                .SendAsync<UdpTrackerResponse>(updRequest, _trackerId, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug(ex, "Couldn't announce to {trackerUrl}", announceUrl);

            return null;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (iPEndPoint is not null && _lastResponse is not null)
        {
            using var cts = new CancellationTokenSource(5.Seconds);
            await TryAnnounceAsync(iPEndPoint, Events.Stopped, cts.Token).ConfigureAwait(false);
        }
    }
}
