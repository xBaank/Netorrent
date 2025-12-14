using System.Net;
using System.Net.Sockets;

namespace Netorrent.Extensions;

internal static class TcpListenerExtensions
{
    extension(TcpListener)
    {
        public static TcpListener GetFreeTcpListener()
        {
            var listener = new TcpListener(IPAddress.IPv6Any, 0);
            listener.Server.DualMode = true;
            listener.Start();
            return listener;
        }
    }
}
