using System.Net;
using System.Net.Sockets;
using Netorrent.TorrentFile;

namespace Netorrent.Extensions;

internal static class HttpClientExtensions
{
    extension(HttpClient)
    {
        public static HttpClient CreateHttpClient(AddressFamily addressFamily) =>
            new(
                new SocketsHttpHandler()
                {
                    ConnectCallback = async (context, cancellationToken) =>
                    {
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
