using System.Net;
using System.Net.Sockets;
using Netorrent.TorrentFile;

namespace Netorrent.Extensions;

internal static class TcpListenerExtensions
{
    extension(TcpListener)
    {
        public static TcpListener GetFreeTcpListener(
            UsedAdressProtocol usedAdressProtocol,
            int port = 0
        )
        {
            var ipAddress = usedAdressProtocol.ToIpAddress();
            var listener = new TcpListener(ipAddress, port);

            if (
                usedAdressProtocol.HasFlag(UsedAdressProtocol.Ipv4)
                && usedAdressProtocol.HasFlag(UsedAdressProtocol.Ipv6)
            )
            {
                listener.Server.DualMode = true;
            }

            return listener;
        }
    }
}
