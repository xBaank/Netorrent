using System.Buffers;
using System.Collections;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Lazy;
using Netorrent.P2P.Structs;
using TimeSpanXt;

namespace Netorrent.P2P;

internal class PeerConnection(
    TcpClient tcpClient,
    IPEndPoint iPEndPoint,
    BitArray myBitField,
    bool amChocking = true,
    bool amInterested = false,
    bool peerChocking = true,
    bool peerInterested = false
) : IDisposable
{
    [Lazy]
    private NetworkStream Stream => TcpClient.GetStream();

    public TcpClient TcpClient { get; } = tcpClient;
    public IPEndPoint IPEndPoint { get; } = iPEndPoint;
    public BitArray MyBitField { get; } = myBitField;
    public bool AmChocking { get; private set; } = amChocking;
    public bool AmInterested { get; private set; } = amInterested;
    public bool PeerChocking { get; private set; } = peerChocking;
    public bool PeerInterested { get; private set; } = peerInterested;
    public string? PeerId { get; private set; }
    public BitArray PeerBitField { get; private set; } = new(0);

    public async Task HandleOutgoing(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            if (AmChocking)
            {
                await Task.Delay(500, cancellationToken);
                continue;
            }

            // Implement logic to send messages to the peer
            await Task.Delay(1000, cancellationToken); // Placeholder delay
        }
    }

    public async Task HandleIncoming(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            if (PeerChocking)
            {
                await Task.Delay(500, cancellationToken);
                continue;
            }

            using var message = await ReceiveMessage(cancellationToken);

            if (message.Id == 255) // Keep-alive
                continue;

            if (message.Id == Message.Bitfield)
            {
                var bitfieldBytes = message.Payload!.Value.Memory;
                PeerBitField = new BitArray(bitfieldBytes.ToArray());
            }

            if (message.Id == Message.Interested)
            {
                PeerInterested = true;
            }

            if (message.Id == Message.NotInterested)
            {
                PeerInterested = false;
            }

            if (message.Id == Message.Choke)
            {
                PeerChocking = true;
            }

            if (message.Id == Message.Unchoke)
            {
                PeerChocking = false;
            }

            if (message.Id == Message.Have)
            {
                message.Payload!.Value.Memory.Span.Reverse();

                var pieceIndex = BitConverter.ToInt32(message.Payload!.Value.Memory.Span);
                if (pieceIndex >= PeerBitField.Length)
                {
                    var newBitField = new BitArray(pieceIndex + 1);
                    for (int i = 0; i < PeerBitField.Length; i++)
                    {
                        newBitField[i] = PeerBitField[i];
                    }
                    PeerBitField = newBitField;
                }
                PeerBitField[pieceIndex] = true;
            }

            if (message.Id == Message.Request)
            {
                // Handle request message
            }

            if (message.Id == Message.Piece)
            {
                // Handle piece message
            }

            if (message.Id == Message.Cancel)
            {
                // Handle cancel message
            }

            if (message.Id == Message.Port)
            {
                //TODO Implement DHT port message handling
            }
        }
    }

    public async Task PerformHandshakeAsync(
        ReadOnlyMemory<byte> infoHash,
        string peerId,
        CancellationToken cancellationToken = default
    )
    {
        using var timeoutCts = new CancellationTokenSource(10.Seconds());
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            timeoutCts.Token
        );
        using var bytesRented = await SendHandHandshake(infoHash, peerId, linkedCts.Token);
        var (pool, receivedHandshake) = await ReceiveHandshake(linkedCts.Token);

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
        using var timeoutCts = new CancellationTokenSource(10.Seconds());
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            timeoutCts.Token
        );

        var (pool, receivedHandshake) = await ReceiveHandshake(linkedCts.Token);

        using (pool)
        {
            using var bytesRented = await SendHandHandshake(infoHash, peerId, linkedCts.Token);
            ValidateHandshake(infoHash, receivedHandshake);
        }
    }

    public async ValueTask SendMessage(
        Message message,
        CancellationToken cancellationToken = default
    )
    {
        using var messageBytes = message.ToBytes();
        await Stream.WriteAsync(messageBytes.Memory, cancellationToken);
        await Stream.FlushAsync(cancellationToken);
    }

    public async ValueTask<Message> ReceiveMessage(CancellationToken cancellationToken = default)
    {
        using var lengthPool = MemoryPool<byte>.Shared.Rent(4);
        var lengthBuffer = lengthPool.Memory[..4];
        await Stream.ReadExactlyAsync(lengthBuffer, cancellationToken);
        int messageLength = BitConverter.ToInt32(
            lengthBuffer.Span[..4].ToArray().Reverse().ToArray()
        );
        if (messageLength == 0)
            return Message.CreateKeepAlive();
        using var messagePool = MemoryPool<byte>.Shared.Rent(messageLength);
        var messageBuffer = messagePool.Memory[..messageLength];
        await Stream.ReadExactlyAsync(messageBuffer, cancellationToken);
        return Message.FromBytes(messageBuffer.Span);
    }

    public async Task SendBitfieldAsync(BitArray bitField, CancellationToken cancellationToken)
    {
        if (!bitField.HasAnySet())
            return;

        int byteCount = (bitField.Length + 7) / 8;
        var owner = MemoryPool<byte>.Shared.Rent(byteCount);
        var memory = owner.Memory[..byteCount];
        PackBitsBigEndian(bitField, memory.Span);

        var message = Message.CreateBitfield(new MemoryRented<byte>(owner, byteCount));
        await SendMessage(message, cancellationToken);
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
        var buffer = pool.Memory[..Handshake.TotalLength];
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

    static void PackBitsBigEndian(BitArray bits, Span<byte> dest)
    {
        int byteLen = (bits.Length + 7) / 8;
        if (dest.Length < byteLen)
            throw new ArgumentException("dest too small", nameof(dest));
        dest[..byteLen].Clear();

        for (int i = 0; i < bits.Length; i++)
        {
            if (bits[i])
                dest[i / 8] |= (byte)(1 << (7 - (i % 8)));
        }
    }

    public void Dispose()
    {
        TcpClient.Close();
        TcpClient.Dispose();
    }
}
