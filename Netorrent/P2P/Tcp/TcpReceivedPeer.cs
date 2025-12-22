using System.Net;
using System.Net.Sockets;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Messages;

namespace Netorrent.P2P.Tcp;

internal class TcpReceivedPeer(
    TcpClient tcpClient,
    IPEndPoint iPEndPoint,
    PeerId peerId,
    Handshake handshake
) : IPeer
{
    public IPEndPoint PeerEndPoint => iPEndPoint;
    private TcpMessageStream? _tcpMessageStream;

    public async ValueTask<IMessageStream> ConnectAsync(CancellationToken cancellationToken)
    {
        if (_tcpMessageStream is not null)
        {
            return _tcpMessageStream;
        }

        tcpClient = new TcpClient();
        await tcpClient.ConnectAsync(iPEndPoint, cancellationToken).ConfigureAwait(false);

        handshake = await Handshake
            .PerformHandshakeAsync(
                tcpClient.GetStream(),
                handshake.InfoHash,
                peerId,
                cancellationToken
            )
            .ConfigureAwait(false);

        return _tcpMessageStream = tcpClient.GetMessageStream(handshake);
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
