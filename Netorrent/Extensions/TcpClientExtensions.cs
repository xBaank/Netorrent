using System.Net.Sockets;
using Netorrent.IO;
using Netorrent.P2P;

namespace Netorrent.Extensions;

internal static class TcpClientExtensions
{
    extension(TcpClient tcpClient)
    {
        public TcpMessageStream GetMessageStream(PeerId peerId) => new(tcpClient, peerId);
    }
}
