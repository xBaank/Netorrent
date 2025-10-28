using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Lazy;
using Netorrent.P2P.Structs;

namespace Netorrent.P2P;

internal record PeerConnection(
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
        using var bytesRented = await SendHandHandshake(infoHash, peerId, cancellationToken);
        var (pool, receivedHandshake) = await ReceiveHandshake(cancellationToken);

        using (pool)
        {
            ValidateHandshake(infoHash, receivedHandshake);
        }
    }

    public async Task ReceiveHandshakeAsync(
        ReadOnlyMemory<byte> infoHash,
        string peerId,
        CancellationToken cancellationToken = default
    )
    {
        var (pool, receivedHandshake) = await ReceiveHandshake(cancellationToken);

        using (pool)
        {
            using var bytesRented = await SendHandHandshake(infoHash, peerId, cancellationToken);
            ValidateHandshake(infoHash, receivedHandshake);
        }
    }

    public async Task SendMessage(Message message, CancellationToken cancellationToken = default)
    {
        using var messageBytes = message.ToBytes();
        await Stream.WriteAsync(messageBytes.Memory, cancellationToken);
        await Stream.FlushAsync(cancellationToken);
    }

    private void ValidateHandshake(ReadOnlyMemory<byte> infoHash, Handshake receivedHandshake)
    {
        if (receivedHandshake.InfoHash.AsSpan().SequenceEqual(infoHash.Span) is false)
            throw new InvalidDataException("InfoHash mismatch in handshake.");

        PeerId = receivedHandshake.PeerId;
    }

    private async Task<(IMemoryOwner<byte> pool, Handshake receivedHandshake)> ReceiveHandshake(
        CancellationToken cancellationToken
    )
    {
        using var pool = MemoryPool<byte>.Shared.Rent(Handshake.TotalLength);
        var buffer = pool.Memory.Slice(0, Handshake.TotalLength);
        await Stream.ReadExactlyAsync(buffer, cancellationToken);
        var receivedHandshake = Handshake.FromBytes(buffer.Span);
        return (pool, receivedHandshake);
    }

    private async Task<MemoryRented<byte>> SendHandHandshake(
        ReadOnlyMemory<byte> infoHash,
        string peerId,
        CancellationToken cancellationToken
    )
    {
        var handshake = Handshake.Create(infoHash.ToArray(), Encoding.ASCII.GetBytes(peerId));
        var bytesRented = handshake.ToBytes();
        await Stream.WriteAsync(bytesRented.Memory, cancellationToken);
        await Stream.FlushAsync(cancellationToken);
        return bytesRented;
    }

    public void Dispose()
    {
        TcpClient.Close();
        TcpClient.Dispose();
    }
}
