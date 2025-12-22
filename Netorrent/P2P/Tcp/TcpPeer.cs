using System.Net;
using System.Net.Sockets;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Messages;

namespace Netorrent.P2P.Tcp;

internal class TcpPeer(IPEndPoint iPEndPoint, PeerId peerId, ReadOnlyMemory<byte> infoHash) : IPeer
{
    public IPEndPoint PeerEndPoint => iPEndPoint;
    private TcpMessageStream? _tcpMessageStream;

    public async ValueTask<IMessageStream> ConnectAsync(CancellationToken cancellationToken)
    {
        if (_tcpMessageStream is not null)
        {
            return _tcpMessageStream;
        }

        var tcpClient = new TcpClient();
        using var cts = cancellationToken.WithTimeout(10.Seconds);
        await tcpClient.ConnectAsync(iPEndPoint, cts.Token).ConfigureAwait(false);
        var stream = tcpClient.GetStream();

        var handshake = await Handshake
            .PerformHandshakeAsync(tcpClient.GetStream(), infoHash, peerId, cancellationToken)
            .ConfigureAwait(false);

        return tcpClient.GetMessageStream(handshake);
    }

    public async ValueTask DisconnectAsync(CancellationToken cancellationToken)
    {
        if (_tcpMessageStream is not null)
        {
            await _tcpMessageStream.DisposeAsync().ConfigureAwait(false);
        }

        _tcpMessageStream = null;
    }
}
