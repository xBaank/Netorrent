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
            var listeners = new List<TcpListener>(ipAddresses.Length);

            try
            {
                foreach (var ipAddress in ipAddresses)
                {
                    var usedPort = port ?? 0;
                    var listener = new TcpListener(ipAddress, usedPort);
                    if (ipAddress.AddressFamily == AddressFamily.InterNetworkV6)
                    {
                        listener.Server.DualMode = false;
                    }
                    listener.Start();
                    port ??= ((IPEndPoint)listener.LocalEndpoint).Port;
                    listeners.Add(listener);
                }

                return listeners;
            }
            catch
            {
                foreach (var l in listeners)
                {
                    try
                    {
                        l.Stop();
                    }
                    catch { }
                }
                throw;
            }
        }
    }
}
