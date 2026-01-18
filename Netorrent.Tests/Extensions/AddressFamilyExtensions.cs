using System.Net;
using System.Net.Sockets;

namespace Netorrent.Tests.Extensions;

internal static class AddressFamilyExtensions
{
    extension(AddressFamily addressFamily)
    {
        public IPAddress BindIp() =>
            addressFamily switch
            {
                AddressFamily.InterNetwork => IPAddress.Any,
                AddressFamily.InterNetworkV6 => IPAddress.IPv6Any,
                _ => IPAddress.Any,
            };
    }
}
