using System.Net;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.IO;
using Netorrent.P2P;
using Netorrent.Tracker.Udp.Request;
using Netorrent.Tracker.Udp.Response;
using TimeSpanXt;

namespace Netorrent.Tracker.Udp;

internal class UdpTracker(
    UdpTrackerTransactionManager transactionManager,
    P2PClient p2PClient,
    PeerId peerId,
    ChannelWriter<IPEndPoint> channelWriter,
    byte[] infoHash,
    string announceUrl,
    ILogger logger,
    IPAddress? forcedIp
) : ITracker
{
    public Task? TrackerTask { get; private set; }
    private CancellationTokenSource? _cancellationTokenSource;
    private IPEndPoint? _ipEndPoint;

    public void Start(CancellationToken cancellationToken) =>
        TrackerTask ??= ProcessLoop(cancellationToken);

    public async Task ProcessLoop(CancellationToken cancellationToken)
    {
        _cancellationTokenSource ??= CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken
        );

        var uri = new Uri(announceUrl);
        var ips = await Dns.GetHostAddressesAsync(uri.Host, _cancellationTokenSource.Token);

        if (ips.Length == 0)
            throw new Exception();

        _ipEndPoint = new IPEndPoint(ips[0], uri.Port);

        var response = await TryAnnounce(
            _ipEndPoint,
            Events.Started,
            _cancellationTokenSource.Token
        );

        if (response is null)
            return;

        foreach (var peer in response.Peers)
        {
            await channelWriter.WriteAsync(peer, cancellationToken);
        }

        while (!_cancellationTokenSource.Token.IsCancellationRequested)
        {
            await Task.Delay(response.Interval.Seconds(), _cancellationTokenSource.Token);

            var interval = response.Interval.Seconds();

            if (logger.IsEnabled(LogLevel.Trace))
                logger.LogTrace("Waiting {seconds} seconds", interval.TotalSeconds);

            var newResponse = await TryAnnounce(_ipEndPoint, null, _cancellationTokenSource.Token);

            if (newResponse is null)
                continue;

            response = newResponse;

            foreach (var peer in response.Peers)
            {
                await channelWriter.WriteAsync(peer, cancellationToken);
            }
        }
    }

    public async Task<UdpTrackerResponse?> TryAnnounce(
        IPEndPoint iPEndPoint,
        string? @event = null,
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("Announcing to {url}", announceUrl);

            var updRequest = new UdpTrackerRequest(
                iPEndPoint,
                infoHash,
                peerId,
                (long)p2PClient.FileManager.GetWrittenBytes(),
                (long)p2PClient.FileManager.GetWrittenBytes(),
                0, //TODO implement
                @event,
                (ushort)p2PClient.EndPoint.Port,
                NumWant: 50,
                IpAddress: forcedIp
            );

            return await transactionManager.SendAsync<UdpTrackerResponse>(
                updRequest,
                cancellationToken
            );
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
        _cancellationTokenSource?.Cancel();
        if (_ipEndPoint is not null)
            await TryAnnounce(_ipEndPoint, Events.Stopped);
    }
}
