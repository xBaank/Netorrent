using System.Net;
using System.Net.Sockets;

namespace Netorrent.Tracker.Udp.Client;

internal class UdpClientWrapper(UdpClient udpClient) : IUdpClient
{
    public ValueTask<UdpReceiveResult> ReceiveAsync(CancellationToken cancellationToken) =>
        udpClient.ReceiveAsync(cancellationToken);

    public ValueTask<int> SendAsync(
        ReadOnlyMemory<byte> datagram,
        IPEndPoint endPoint,
        CancellationToken cancellationToken
    ) => udpClient.SendAsync(datagram, endPoint, cancellationToken);
}
