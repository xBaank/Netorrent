using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Lazy;
using Netorrent.P2P.Structs;

namespace Netorrent.P2P;

public record PeerConnection(
    TcpClient TcpClient,
    IPEndPoint IPEndPoint,
    bool AmChocking = true,
    bool AmInterested = false,
    bool PeerChocking = true,
    bool PeerInterested = false
) : IDisposable
{
    [Lazy]
    private NetworkStream Stream => TcpClient.GetStream();

    public string? PeerId { get; private set; }

    //TODO merge in one method
    public async Task PerformHandshakeAsync(
        ReadOnlyMemory<byte> infoHash,
        string peerId,
        CancellationToken cancellationToken = default
    )
    {
        var handshake = Handshake.Create(infoHash.ToArray(), Encoding.ASCII.GetBytes(peerId));
        using var bytesRented = handshake.ToBytes();
        await Stream.WriteAsync(bytesRented.Memory, cancellationToken);
        await Stream.FlushAsync(cancellationToken);

        using var pool = MemoryPool<byte>.Shared.Rent(Handshake.TotalLength);
        var buffer = pool.Memory[Handshake.TotalLength..];
        await Stream.ReadExactlyAsync(buffer, cancellationToken);
        var receivedHandshake = Handshake.FromBytes(buffer.Span);

        if (receivedHandshake.InfoHash.AsSpan().SequenceEqual(infoHash.Span) is false)
            throw new InvalidDataException("InfoHash mismatch in handshake.");

        PeerId = receivedHandshake.PeerId;
    }

    public async Task ReceiveHandshakeAsync(
        ReadOnlyMemory<byte> infoHash,
        string peerId,
        CancellationToken cancellationToken = default
    )
    {
        using var pool = MemoryPool<byte>.Shared.Rent(Handshake.TotalLength);
        var buffer = pool.Memory[Handshake.TotalLength..];
        await Stream.ReadExactlyAsync(buffer, cancellationToken);
        var receivedHandshake = Handshake.FromBytes(buffer.Span);

        var handshake = Handshake.Create(infoHash.ToArray(), Encoding.ASCII.GetBytes(peerId));
        using var bytesRented = handshake.ToBytes();
        await Stream.WriteAsync(bytesRented.Memory, cancellationToken);
        await Stream.FlushAsync(cancellationToken);

        if (receivedHandshake.InfoHash.AsSpan().SequenceEqual(infoHash.Span) is false)
            throw new InvalidDataException("InfoHash mismatch in handshake.");

        PeerId = receivedHandshake.PeerId;
    }

    public void Dispose()
    {
        TcpClient.Close();
        TcpClient.Dispose();
    }
}
