using System.Net;
using System.Net.Sockets;
using Netorrent.TorrentFile;

namespace Netorrent.Extensions;

internal static class UdpClientExtensions
{
    extension(UdpClient)
    {
        public static UdpClient GetFreeUdpClient(
            UsedAddressProtocol usedAdressProtocol,
            int port = 0
        )
        {
            var ipAdress = usedAdressProtocol.BindIpAddress();
            var udpClient = new UdpClient(ipAdress.AddressFamily);

            if (
                usedAdressProtocol.HasFlag(UsedAddressProtocol.Ipv4)
                && usedAdressProtocol.HasFlag(UsedAddressProtocol.Ipv6)
            )
            {
                udpClient.Client.DualMode = true;
            }

            udpClient.Client.Bind(new IPEndPoint(ipAdress, port));
            return udpClient;
        }
    }
}
