using System.Buffers;
using System.Buffers.Binary;
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

    private const int PEER_REQUEST_LIMIT = 8;
    private const int MAX_IGNORED_REQUESTS = 16;
    private const int MAX_VIOLATION_COUNT = 16;
    private const int MAX_BLOCK_LENGTH = 16 * 1024;
    private readonly FileManager _fileManager = fileManager;
    private readonly RequestManager _requestManager = requestManager;
    private readonly Dictionary<Request, CancellationTokenSource> _pendingRequests = [];
    private int _violationCount = 0;
    private int _ignoredRequests = 0;

    public async Task HandleOutgoing(CancellationToken cancellationToken)
    {
        using var message = Message.CreateInterested();
        await SendMessage(message, cancellationToken);
        AmInterested = true;
        MyBitField.OnHavePieceAsync += SendHave;

        while (!cancellationToken.IsCancellationRequested)
        {
            //SEEDER LOGIC
            if (_requestManager.IsChoking || _ignoredRequests > MAX_IGNORED_REQUESTS)
            {
                AmChocking = true;
                using var chokeMessage = Message.CreateChoke();
                await SendMessage(message, cancellationToken);
            }

            if (_requestManager.ShouldUnchoke)
            {
                AmChocking = false;
            }

            await ProcessPiece(cancellationToken);

            //LEECHER LOGIC
        }
    }

    private async Task SendHave(int pieceIndex, CancellationToken cancellationToken)
    {
        using var haveMessage = Message.CreateHave(pieceIndex);
        await SendMessage(haveMessage, cancellationToken);
    }

    public async Task HandleIncoming(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
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
                await PeerBitField.HavePiece(pieceIndex, cancellationToken);
            }

            if (
                message.Id == Message.Request
                && !AmChocking
                && _pendingRequests.Count <= PEER_REQUEST_LIMIT
            )
            {
                if (!(await ReceiveRequest(message, cancellationToken)))
                {
                    // Invalid request, handle accordingly
                }
            }
            else
            {
                if (AmChocking)
                    _violationCount++;
                else
                    _ignoredRequests++;

                if (_violationCount >= MAX_VIOLATION_COUNT)
                {
                    throw new InvalidOperationException("Peer violated 3 times");
                }
            }

            if (message.Id == Message.Piece)
            {
                await ReceivePiece(message, cancellationToken);
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

    private async ValueTask<bool> ReceiveRequest(
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
        if (length <= 0 || length > MAX_BLOCK_LENGTH)
            return false;

        var cts = new CancellationTokenSource();
        var request = new Request(index, begin, length, cts.Token);
        await _requestManager.AddRequestAsync(request, cancellationToken);
        _pendingRequests[request] = cts;
        return true;
    }

    private async Task ProcessPiece(CancellationToken cancellationToken)
    {
        var request = await _requestManager.GetNextRequestAsync(cancellationToken);
        if (!request.CancellationToken.IsCancellationRequested)
        {
            var pieceData = await _fileManager.ReadPieceAsync(
                request.Index,
                request.Begin,
                request.Length,
                cancellationToken
            );

            using var pieceMessage = Message.CreatePiece(request.Index, request.Begin, pieceData);

            await SendMessage(pieceMessage, cancellationToken);
            _pendingRequests.Remove(request);
        }
    }

    private async Task ReceivePiece(Message message, CancellationToken cancellationToken)
    {
        var span = message.Payload!.Value.Memory.Span;
        var index = BinaryPrimitives.ReadInt32BigEndian(span[..4]);
        var begin = BinaryPrimitives.ReadInt32BigEndian(span[4..8]);
        var pieceData = message.Payload!.Value.Memory[8..];
        await _fileManager.WritePieceAsync(index, begin, pieceData, cancellationToken);
        await MyBitField.HavePiece(index, cancellationToken);
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
        using var message = Message.CreateBitfield(memoryRented);
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

    public void Dispose()
    {
        MyBitField.OnHavePieceAsync -= SendHave;
        TcpClient.Close();
        TcpClient.Dispose();
    }
}
