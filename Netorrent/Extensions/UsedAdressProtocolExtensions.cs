using System.Net;
using System.Net.Sockets;
using Netorrent.TorrentFile;

namespace Netorrent.Extensions;

internal static class UsedAdressProtocolExtensions
{
    extension(UsedAddressProtocol usedAdressProtocol)
    {
        public IPAddress ToIpAddress() =>
            usedAdressProtocol switch
            {
                UsedAddressProtocol.Ipv4 => IPAddress.Any,
                UsedAddressProtocol.Ipv6 => IPAddress.IPv6Any,
                _ => IPAddress.IPv6Any,
            };

        public AddressFamily[] ToAddressFamily()
        {
            List<AddressFamily> addressFamilies = [];
            if (usedAdressProtocol.HasFlag(UsedAddressProtocol.Ipv4))
            {
                addressFamilies.Add(AddressFamily.InterNetwork);
            }
            if (usedAdressProtocol.HasFlag(UsedAddressProtocol.Ipv6))
            {
                addressFamilies.Add(AddressFamily.InterNetworkV6);
            }
            return addressFamilies.ToArray();
        }
    }
}
