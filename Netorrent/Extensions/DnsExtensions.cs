using System.Net;

namespace Netorrent.Extensions;

internal static class DnsExtensions
{
    extension(Dns)
    {
        public static async Task<IPAddress[]> GetHostAdressesOrEmptyAsync(
            string hostName,
            CancellationToken cancellationToken
        )
        {
            try
            {
                return await Dns.GetHostAddressesAsync(hostName, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch
            {
                return [];
            }
        }
    }
}
