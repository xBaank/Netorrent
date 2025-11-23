using System.Net;
using System.Net.Sockets;

namespace Netorrent.Other;

internal static class Udp
{
    public static UdpClient GetFreeUdpClient()
    {
        var udpClient = new UdpClient(AddressFamily.InterNetworkV6);
        udpClient.Client.DualMode = true;
        udpClient.Client.Bind(new IPEndPoint(IPAddress.IPv6Any, 0));
        return udpClient;
    }
}
