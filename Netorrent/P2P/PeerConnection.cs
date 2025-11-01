using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Lazy;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Structs;
using TimeSpanXt;

namespace Netorrent.P2P;

internal class PeerConnection(
    TcpClient tcpClient,
    IPEndPoint iPEndPoint,
    Bitfield myBitField,
    FileManager fileManager,
    RequestManager requestManager,
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
    public Bitfield MyBitField { get; } = myBitField;
    public bool AmChocking { get; private set; } = amChocking;
    public bool AmInterested { get; private set; } = amInterested;
    public bool PeerChocking { get; private set; } = peerChocking;
    public bool PeerInterested { get; private set; } = peerInterested;
    public string? PeerId { get; private set; }
    public Bitfield PeerBitField { get; private set; } = new(myBitField.Length);

    private readonly FileManager _fileManager = fileManager;
    private readonly RequestManager _requestManager = requestManager;
    private readonly Dictionary<Request, CancellationTokenSource> _pendingRequests = [];

    public async Task HandleOutgoing(CancellationToken cancellationToken)
    {
        await SendMessage(Message.CreateInterested(), cancellationToken);
        AmInterested = true;

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
                PeerBitField = new Bitfield(bitfieldBytes.ToArray());
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
                int pieceIndex = BinaryPrimitives.ReadInt32BigEndian(
                    message.Payload!.Value.Memory.Span
                );
                PeerBitField.HavePiece(pieceIndex);
            }

            if (message.Id == Message.Request)
            {
                if (!(await ProcessRequest(message, cancellationToken)))
                {
                    // Invalid request, handle accordingly
                }
            }

            if (message.Id == Message.Piece)
            {
                // Handle piece message
            }

            if (message.Id == Message.Cancel)
            {
                if (!ProcessCancel(message))
                {
                    // Invalid cancel, handle accordingly
                }
            }

            if (message.Id == Message.Port)
            {
                //TODO Implement DHT port message handling
            }
        }
    }

    private async ValueTask<bool> ProcessRequest(
        Message message,
        CancellationToken cancellationToken
    )
    {
        var span = message.Payload!.Value.Memory.Span;
        var index = BinaryPrimitives.ReadInt32BigEndian(span[..4]);
        var begin = BinaryPrimitives.ReadInt32BigEndian(span[4..8]);
        var length = BinaryPrimitives.ReadInt32BigEndian(span[8..12]);

        if (index < 0)
            return false;
        if (index >= length)
            return false;

        var cts = new CancellationTokenSource();
        var request = new Request(index, begin, length, cts.Token);
        await _requestManager.AddRequestAsync(request, cancellationToken);
        _pendingRequests[request] = cts;
        return true;
    }

    private bool ProcessCancel(Message message)
    {
        var span = message.Payload!.Value.Memory.Span;
        var index = BinaryPrimitives.ReadInt32BigEndian(span[..4]);
        var begin = BinaryPrimitives.ReadInt32BigEndian(span[4..8]);
        var length = BinaryPrimitives.ReadInt32BigEndian(span[8..12]);

        var request = new Request(index, begin, length, CancellationToken.None);

        if (_pendingRequests.TryGetValue(request, out CancellationTokenSource? value))
        {
            value.Cancel();
            _pendingRequests.Remove(request);
            return true;
        }
        else
        {
            return false;
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
        using var cts = cancellationToken.WithTimeout(10.Seconds());
        using var messageBytes = message.ToBytes();
        await Stream.WriteAsync(messageBytes.Memory, cts.Token);
        await Stream.FlushAsync(cts.Token);
    }

    public async ValueTask<Message> ReceiveMessage(CancellationToken cancellationToken = default)
    {
        using var cts = cancellationToken.WithTimeout(10.Seconds());
        using var lengthPool = MemoryPool<byte>.Shared.Rent(4);
        var lengthBuffer = lengthPool.Memory[..4];
        await Stream.ReadExactlyAsync(lengthBuffer, cts.Token);
        int messageLength = BitConverter.ToInt32(
            lengthBuffer.Span[..4].ToArray().Reverse().ToArray()
        );
        if (messageLength == 0)
            return Message.CreateKeepAlive();
        using var messagePool = MemoryPool<byte>.Shared.Rent(messageLength);
        var messageBuffer = messagePool.Memory[..messageLength];
        await Stream.ReadExactlyAsync(messageBuffer, cts.Token);
        return Message.FromBytes(messageBuffer.Span);
    }

    public async Task SendBitfieldAsync(Bitfield bitField, CancellationToken cancellationToken)
    {
        using var memoryRented = bitField.ToMemoryRented();
        var message = Message.CreateBitfield(memoryRented);
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
        using var cts = cancellationToken.WithTimeout(10.Seconds());
        using var pool = MemoryPool<byte>.Shared.Rent(Handshake.TotalLength);
        var buffer = pool.Memory[..Handshake.TotalLength];
        await Stream.ReadExactlyAsync(buffer, cts.Token);
        var receivedHandshake = Handshake.FromBytes(buffer.Span);
        return (pool, receivedHandshake);
    }

    private async Task<MemoryRented<byte>> SendHandHandshake(
        ReadOnlyMemory<byte> infoHash,
        string peerId,
        CancellationToken cancellationToken
    )
    {
        using var cts = cancellationToken.WithTimeout(10.Seconds());
        var handshake = Handshake.Create(infoHash.ToArray(), Encoding.ASCII.GetBytes(peerId));
        var bytesRented = handshake.ToBytes();
        await Stream.WriteAsync(bytesRented.Memory, cts.Token);
        await Stream.FlushAsync(cts.Token);
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
