using System.Net;
using System.Net.Sockets;
using Netorrent.TorrentFile;

namespace Netorrent.Extensions;

internal static class UsedAddressProtocolExtensions
{
    extension(UsedAddressProtocol usedAdressProtocol)
    {
        public IPAddress BindIpAddress() =>
            usedAdressProtocol switch
            {
                UsedAddressProtocol.Ipv4 => IPAddress.Any,
                UsedAddressProtocol.Ipv6 => IPAddress.IPv6Any,
                UsedAddressProtocol.Ipv6 | UsedAddressProtocol.Ipv4 => IPAddress.IPv6Any,
                _ => throw new ArgumentException(
                    "Unsupported Address Protocol",
                    nameof(usedAdressProtocol)
                ),
            };

        public IReadOnlySet<AddressFamily> SupportedAddressFamilies()
        {
            HashSet<AddressFamily> addressFamilies = new(2);
            if (usedAdressProtocol.HasFlag(UsedAddressProtocol.Ipv4))
            {
                addressFamilies.Add(AddressFamily.InterNetwork);
            }
            if (usedAdressProtocol.HasFlag(UsedAddressProtocol.Ipv6))
            {
                addressFamilies.Add(AddressFamily.InterNetworkV6);
            }
            return addressFamilies;
        }
    }
}
