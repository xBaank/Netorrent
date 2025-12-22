using System.Net;
using Netorrent.IO;

namespace Netorrent.P2P;

interface IPeer
{
    IPEndPoint PeerEndPoint { get; }
    public ValueTask<IMessageStream> ConnectAsync(CancellationToken cancellationToken);
    public ValueTask DisconnectAsync(CancellationToken cancellationToken);
}
