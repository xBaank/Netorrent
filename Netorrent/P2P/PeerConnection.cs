using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Lazy;

namespace Netorrent.P2P;

public record PeerConnection(
    TcpClient TcpClient,
    IPEndPoint IPEndPoint,
    bool AmChocking = true,
    bool AmInterested = false,
    bool PeerChocking = true,
    bool PeerInterested = false
)
{
    private static readonly byte[] pstrlen = [19];
    private static readonly byte[] protocol = Encoding.ASCII.GetBytes("BitTorrent protocol");
    private static readonly byte[] reserved = [0, 0, 0, 0, 0, 0, 0, 0];

    [Lazy]
    private NetworkStream Stream => TcpClient.GetStream();

    public string? PeerId { get; private set; }

    public async Task PerformHandshakeAsync(
        ReadOnlyMemory<byte> infoHash,
        string peerId,
        CancellationToken cancellationToken = default
    )
    {
        await Stream.WriteAsync(pstrlen, cancellationToken);
        await Stream.WriteAsync(protocol, cancellationToken);
        await Stream.WriteAsync(reserved, cancellationToken);
        await Stream.WriteAsync(infoHash, cancellationToken);
        await Stream.WriteAsync(Encoding.ASCII.GetBytes(peerId), cancellationToken);
        await Stream.FlushAsync(cancellationToken);

        var len = Stream.ReadByte();
        using var pool = MemoryPool<byte>.Shared.Rent(1 + len + 8 + 20 + 20);
        var buffer = pool.Memory;
        buffer.Span[0] = (byte)len;
        await Stream.ReadExactlyAsync(buffer.Slice(1, len + 8 + 20 + 20), cancellationToken);
        var receivedProtocol = Encoding.ASCII.GetString(buffer.Span.Slice(1, len));
        var receivedReserved = buffer.Span.Slice(1 + len, 8);
        var receivedInfoHash = buffer.Span.Slice(1 + len + 8, 20);
        var receivedPeerId = Encoding.ASCII.GetString(buffer.Span.Slice(1 + len + 8 + 20, 20));

        if (infoHash.Span.SequenceEqual(receivedInfoHash) is false)
        {
            throw new Exception("InfoHash mismatch in handshake.");
        }

        PeerId = receivedPeerId;
    }
}
