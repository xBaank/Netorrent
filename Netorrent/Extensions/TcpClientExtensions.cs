using System.Net.Sockets;
using Netorrent.IO;
using Netorrent.P2P.Messages;

namespace Netorrent.Extensions;

internal static class TcpClientExtensions
{
    extension(TcpClient tcpClient)
    {
        public TcpMessageStream GetMessageStream(Handshake handshake) => new(tcpClient, handshake);
    }
}
