using System.Net;
using System.Net.Sockets;
using Netorrent.TorrentFile;

namespace Netorrent.Extensions;

internal static class TcpListenerExtensions
{
    extension(TcpListener)
    {
        public static TcpListener GetFreeTcpListener(
            UsedAddressProtocol usedAdressProtocol,
            int port = 0
        )
        {
            var ipAddress = usedAdressProtocol.BindIpAddress();
            var listener = new TcpListener(ipAddress, port);

            if (
                usedAdressProtocol.HasFlag(UsedAddressProtocol.Ipv4)
                && usedAdressProtocol.HasFlag(UsedAddressProtocol.Ipv6)
            )
            {
                listener.Server.DualMode = true;
            }

            return listener;
        }
    }
}
