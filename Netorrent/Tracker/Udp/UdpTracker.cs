using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
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
    private UdpTrackerResponse? _lastResponse;
    private readonly Guid _trackerId = Guid.CreateVersion7();

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

        var ipv4 = ips.FirstOrDefault(i => i.AddressFamily == AddressFamily.InterNetwork)
            ?.MapToIPv6();

        if (ipv4 is null)
            return;

        _ipEndPoint = new IPEndPoint(ipv4, uri.Port);

        await transactionManager.ConnectAsync(
            _ipEndPoint,
            _trackerId,
            _cancellationTokenSource.Token
        );

        _lastResponse = await TryAnnounce(
            _ipEndPoint,
            @event: Events.Started,
            cancellationToken: _cancellationTokenSource.Token
        );

        if (_lastResponse is null)
            return;

        foreach (var peer in _lastResponse.Peers)
        {
            await channelWriter.WriteAsync(peer, cancellationToken);
        }

        while (!_cancellationTokenSource.Token.IsCancellationRequested)
        {
            await Task.Delay(_lastResponse.Interval.Seconds(), _cancellationTokenSource.Token);

            var interval = _lastResponse.Interval.Seconds();

            if (logger.IsEnabled(LogLevel.Trace))
                logger.LogTrace("Waiting {seconds} seconds", interval.TotalSeconds);

            var newResponse = await TryAnnounce(_ipEndPoint, null, _cancellationTokenSource.Token);

            if (newResponse is null)
                continue;

            _lastResponse = newResponse;

            foreach (var peer in _lastResponse.Peers)
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

            var connectionId = transactionManager.GetConnectionIdOrNull(_trackerId);

            if (connectionId is null || transactionManager.IsOutdated(connectionId.Value))
            {
                var response = await transactionManager.ConnectAsync(
                    iPEndPoint,
                    _trackerId,
                    cancellationToken
                );

                connectionId = response.ConnectionId;
            }

            var updRequest = new UdpTrackerRequest(
                iPEndPoint,
                infoHash,
                peerId,
                (long)p2PClient.FileManager.GetWrittenBytes(),
                (long)p2PClient.FileManager.GetWrittenBytes(),
                0, //TODO implement
                @event,
                (ushort)p2PClient.EndPoint.Port,
                ConnectionId: connectionId.Value,
                TransactionId: transactionManager.MakeTransactionId(),
                NumWant: 50,
                IpAddress: forcedIp
            );

            return await transactionManager.SendAsync<UdpTrackerResponse>(
                updRequest,
                _trackerId,
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
        if (_ipEndPoint is not null && _lastResponse is not null)
            await TryAnnounce(_ipEndPoint, @event: Events.Stopped);
    }
}
