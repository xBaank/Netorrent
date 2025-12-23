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
                UsedAdressProtocol.Dual => IPAddress.IPv6Any,
                _ => throw new ArgumentOutOfRangeException(nameof(usedAdressProtocol)),
            };

        public AddressFamily[] ToAddressFamily() =>
            usedAdressProtocol switch
            {
                UsedAdressProtocol.Ipv4 => [AddressFamily.InterNetwork],
                UsedAdressProtocol.Ipv6 => [AddressFamily.InterNetworkV6],
                UsedAdressProtocol.Dual =>
                [
                    AddressFamily.InterNetwork,
                    AddressFamily.InterNetworkV6,
                ],
                _ => throw new ArgumentOutOfRangeException(nameof(usedAdressProtocol)),
            };
    }
}
