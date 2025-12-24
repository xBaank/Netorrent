using System.Net;
using System.Net.Sockets;
using Netorrent.TorrentFile;

namespace Netorrent.Extensions;

internal static class UsedAdressProtocolExtensions
{
    extension(UsedAdressProtocol usedAdressProtocol)
    {
        public IPAddress ToIpAddress() =>
            usedAdressProtocol switch
            {
                UsedAdressProtocol.Ipv4 => IPAddress.Any,
                UsedAdressProtocol.Ipv6 => IPAddress.IPv6Any,
                _ => IPAddress.IPv6Any,
            };

        public AddressFamily[] ToAddressFamily()
        {
            List<AddressFamily> addressFamilies = [];
            if (usedAdressProtocol.HasFlag(UsedAdressProtocol.Ipv4))
            {
                addressFamilies.Add(AddressFamily.InterNetwork);
            }
            if (usedAdressProtocol.HasFlag(UsedAdressProtocol.Ipv6))
            {
                addressFamilies.Add(AddressFamily.InterNetwork);
            }
            return addressFamilies.ToArray();
        }
    }
}
