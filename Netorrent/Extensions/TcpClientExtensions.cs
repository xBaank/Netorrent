using System.Net.Sockets;
using Netorrent.IO;

namespace Netorrent.Extensions;

internal static class TcpClientExtensions
{
    extension(TcpClient tcpClient)
    {
        public TcpMessageStream GetMessageStream() => new(tcpClient);
    }
}
