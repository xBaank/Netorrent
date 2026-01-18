using System.Net;
using System.Net.Sockets;

namespace Netorrent.Extensions;

internal static class TcpListenerExtensions
{
    extension(TcpListener)
    {
        public static List<TcpListener> GetFreeTcpListeners(
            IPAddress[] ipAddresses,
            int? port = null
        )
        {
            List<TcpListener> tcpListeners = new(ipAddresses.Length);

            foreach (var ipAddress in ipAddresses)
            {
                var address = ipAddress;
                var usedPort = port ?? 0;
                var listener = new TcpListener(address, usedPort);
                listener.Start();
                port ??= ((IPEndPoint)listener.LocalEndpoint).Port;
                tcpListeners.Add(listener);
            }
            return tcpListeners;
        }
    }
}
