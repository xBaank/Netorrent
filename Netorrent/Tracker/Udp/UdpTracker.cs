using System.Net;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.P2P;

namespace Netorrent.Tracker.Udp;

internal class UdpTracker(
    UdpTrackerTransactionManager transactionManager,
    P2PClient p2PClient,
    string peerId,
    ChannelWriter<IPEndPoint> trackersChannel,
    byte[] infoHash,
    string announceUrl,
    ILogger logger
) : ITracker
{
    public Task? TrackerTask { get; private set; }

    public void Start(CancellationToken cancellationToken) =>
        TrackerTask ??= ProcessLoop(cancellationToken);

    public async Task ProcessLoop(CancellationToken cancellationToken)
    {
        var uri = new Uri(announceUrl);
        var ips = await Dns.GetHostAddressesAsync(uri.Host, cancellationToken);

        if (ips.Length == 0)
            throw new Exception();

        var ipEndpoint = new IPEndPoint(ips[0], uri.Port);
        try
        {
            await transactionManager.ConnectAsync(ipEndpoint, cancellationToken);
        }
        catch
        {
            logger.LogDebug("Not connected");
        }
        logger.LogDebug("Connected");
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
