using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.P2P;
using Netorrent.Tracker.Http;

namespace Netorrent.Tracker.Udp;

internal class UdpTracker(
    UdpClient udpClient,
    P2PClient p2PClient,
    string peerId,
    ChannelWriter<IPEndPoint> trackersChannel,
    byte[] infoHash,
    string announceUrl,
    ILogger logger
) : ITracker
{
    public Task? TrackerTask => throw new NotImplementedException();

    public void Start(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async Task ProcessLoop(CancellationToken cancellationToken)
    {
        var uri = new Uri(announceUrl);
        string hostname = uri.Host;
        int port = uri.Port;
        await udpClient.SendAsync(new Memory<byte>(), hostname, port, cancellationToken);
        await udpClient.ReceiveAsync(cancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        throw new NotImplementedException();
    }
}
