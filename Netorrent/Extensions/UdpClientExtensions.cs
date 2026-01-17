using System.Net;
using System.Net.Sockets;
using Netorrent.TorrentFile;

namespace Netorrent.Extensions;

internal static class UdpClientExtensions
{
    extension(UdpClient)
    {
        public static UdpClient GetFreeUdpClient(IPAddress iPAddress, int port = 0)
        {
            var udpClient = new UdpClient(iPAddress.AddressFamily);
            udpClient.Client.Bind(new IPEndPoint(iPAddress, port));
            return udpClient;
        }
    }
}
