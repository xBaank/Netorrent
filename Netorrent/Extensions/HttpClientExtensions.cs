using System.Net;
using System.Net.Sockets;

namespace Netorrent.Extensions;

internal static class HttpClientExtensions
{
    extension(HttpClient)
    {
        public static HttpClient CreateHttpClient(IPAddress iPAddress) =>
            new(
                new SocketsHttpHandler()
                {
                    ConnectCallback = async (context, cancellationToken) =>
                    {
                        var entry = await Dns.GetHostEntryAsync(
                            context.DnsEndPoint.Host,
                            iPAddress.AddressFamily,
                            cancellationToken
                        );

                        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp)
                        {
                            NoDelay = true,
                        };

                        socket.Bind(new IPEndPoint(iPAddress, 0));

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
