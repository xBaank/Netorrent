using System.Net;
using System.Net.Sockets;
using ZLinq;

namespace Netorrent.Extensions;

internal static class DnsExtensions
{
    extension(Dns)
    {
        public static async ValueTask<(
            IPAddress? ipv4,
            IPAddress? ipv6
        )> GetHostAdressesOrEmptyAsync(Uri uri, CancellationToken cancellationToken)
        {
            try
            {
                if (uri.Host == "localhost")
                {
                    return (IPAddress.Loopback, IPAddress.IPv6Loopback);
                }

                var ips = await Dns.GetHostAddressesAsync(uri.Host, cancellationToken)
                    .ConfigureAwait(false);

                var ipv4 = ips.AsValueEnumerable()
                    .FirstOrDefault(i => i.AddressFamily == AddressFamily.InterNetwork);

                var ipv6 = ips.AsValueEnumerable()
                    .FirstOrDefault(i => i.AddressFamily == AddressFamily.InterNetworkV6);

                return (ipv4, ipv6);
            }
            catch
            {
                return (null, null);
            }
        }
    }
}
