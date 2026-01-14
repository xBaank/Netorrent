using System.Net;
using System.Net.Sockets;

namespace Netorrent.Tracker.Udp.Client;

internal interface IUdpClient
{
    public ValueTask<UdpReceiveResult> ReceiveAsync(CancellationToken cancellationToken);
    public ValueTask<int> SendAsync(
        ReadOnlyMemory<byte> datagram,
        IPEndPoint endPoint,
        CancellationToken cancellationToken
    );
}
