using System.Net;
using System.Net.Sockets;
using Netorrent.TorrentFile;

namespace Netorrent.Extensions;

internal static class HttpClientExtensions
{
    extension(HttpClient)
    {
        public static HttpClient CreateHttpClient(UsedAddressProtocol usedAdressProtocol) =>
            new(
                new SocketsHttpHandler()
                {
                    ConnectCallback = async (context, cancellationToken) =>
                    {
                        var addressFamily = usedAdressProtocol switch
                        {
                            UsedAddressProtocol.Ipv4 => AddressFamily.InterNetwork,
                            UsedAddressProtocol.Ipv6 => AddressFamily.InterNetworkV6,
                            UsedAddressProtocol.Ipv6 | UsedAddressProtocol.Ipv4 =>
                                AddressFamily.Unspecified,
                            _ => throw new ArgumentException(
                                "Unknown address protocol",
                                nameof(usedAdressProtocol)
                            ),
                        };

                        var entry = await Dns.GetHostEntryAsync(
                            context.DnsEndPoint.Host,
                            addressFamily,
                            cancellationToken
                        );

                        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp)
                        {
                            NoDelay = true,
                        };

                        try
                        {
                            await socket.ConnectAsync(
                                entry.AddressList,
                                context.DnsEndPoint.Port,
                                cancellationToken
                            );

                            return new NetworkStream(socket, ownsSocket: true);
                        }
                        catch
                        {
                            socket.Dispose();
                            throw;
                        }
                    },
                }
            );
    }
}
