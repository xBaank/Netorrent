using System.Net;
using System.Net.Sockets;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Messages;
using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.P2P.Tcp;

internal class TcpPeer(
    TcpMessageStream? tcpMessageStream,
    IPEndPoint iPEndPoint,
    PeerId peerId,
    InfoHash infoHash,
    Bitfield myBitfield
) : IPeer
{
    public IPEndPoint PeerEndPoint => iPEndPoint;
    private TcpMessageStream? _tcpMessageStream = tcpMessageStream;

    public async ValueTask<IMessageStream> ConnectAsync(CancellationToken cancellationToken)
    {
        if (_tcpMessageStream is not null)
        {
            return _tcpMessageStream;
        }

        var tcpClient = new TcpClient();
        using var cts = cancellationToken.WithTimeout(10.Seconds);
        await tcpClient.ConnectAsync(iPEndPoint, cts.Token).ConfigureAwait(false);

        var handshake = await Handshake
            .PerformHandshakeAsync(tcpClient.GetStream(), infoHash, peerId, cts.Token)
            .ConfigureAwait(false);

        return _tcpMessageStream = tcpClient.GetMessageStream(handshake, myBitfield);
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
