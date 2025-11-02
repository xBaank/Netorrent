using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Security.Principal;
using System.Text;
using Lazy;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.Other;
using Netorrent.P2P.Managers.Piece;
using Netorrent.P2P.Managers.Request;
using Netorrent.P2P.Structs;
using TimeSpanXt;

namespace Netorrent.P2P;

internal class PeerConnection(
    TcpClient tcpClient,
    IPEndPoint iPEndPoint,
    Bitfield myBitField,
    FileManager fileManager,
    RequestManager requestManager,
    PieceManager pieceManager,
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
    private int? _currentPieceDownloading;
    private Request[] _blocks = [];

    private readonly FileManager _fileManager = fileManager;
    private readonly RequestManager _requestManager = requestManager;
    private readonly PieceManager _pieceManager = pieceManager;

    public void SetCurrentPieceToDownload(int? index)
    {
        _currentPieceDownloading = index;
        if (index is null)
            return;
        _blocks = _fileManager.GetBlocksByPieceIndex(index.Value);
    }

    public async Task WriteLoop(CancellationToken cancellationToken)
    {
        using var message = Message.CreateInterested();
        await SendMessage(message, cancellationToken);
        AmInterested = true;
        MyBitField.OnHavePieceAsync += SendHave;

        while (!cancellationToken.IsCancellationRequested)
        {
            //SEEDER LOGIC
            if (_requestManager.IsChoking)
            {
                AmChocking = true;
                using var chokeMessage = Message.CreateChoke();
                await SendMessage(message, cancellationToken);
            }

            if (_requestManager.ShouldUnchoke)
            {
                AmChocking = false;
                using var unchokeMessage = Message.CreateUnchoke();
                await SendMessage(message, cancellationToken);
            }

            //LEECHER LOGIC

            await Task.Yield();
        }
    }

    public async Task ReadLoop(CancellationToken cancellationToken)
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

            if (message.Id == Message.Request)
            {
                await ReceiveRequestAsync(message, cancellationToken);
            }

            if (message.Id == Message.Piece)
            {
                await ReceivePieceAsync(message, cancellationToken);
            }

            if (message.Id == Message.Cancel)
            {
                ReceiveCancel(message);
            }

            if (message.Id == Message.Port)
            {
                //TODO Implement DHT port message handling
            }
        }
    }

    public async Task ProcessLoop(CancellationToken cancellationToken)
    {
        await foreach (var item in _requestManager.Requests.WithCancellation(cancellationToken))
        {
            await ProcessPieceAsync(item, cancellationToken);
        }
    }

    private async ValueTask ReceiveRequestAsync(
        Message message,
        CancellationToken cancellationToken
    )
    {
        var span = message.Payload!.Value.Memory.Span;
        var index = BinaryPrimitives.ReadInt32BigEndian(span[..4]);
        var begin = BinaryPrimitives.ReadInt32BigEndian(span[4..8]);
        var length = BinaryPrimitives.ReadInt32BigEndian(span[8..12]);

        var request = new Request(index, begin, length);
        var response = await _requestManager.AddRequestAsync(request, cancellationToken);

        if (response == RequestResponseType.Violation)
            throw new InvalidDataException("Received invalid request from peer.");

        if (response == RequestResponseType.Choked)
            return;

        if (response == RequestResponseType.Ignored)
            return;
    }

    private async ValueTask ProcessPieceAsync(Request request, CancellationToken cancellationToken)
    {
        var pieceData = await _fileManager.ReadPieceAsync(
            request.Index,
            request.Begin,
            request.Length,
            cancellationToken
        );

        using var pieceMessage = Message.CreatePiece(request.Index, request.Begin, pieceData);

        await SendMessage(pieceMessage, cancellationToken);
    }

    private async ValueTask ReceivePieceAsync(Message message, CancellationToken cancellationToken)
    {
        var span = message.Payload!.Value.Memory.Span;
        var index = BinaryPrimitives.ReadInt32BigEndian(span[..4]);
        var begin = BinaryPrimitives.ReadInt32BigEndian(span[4..8]);
        var block = new Block(index, begin, MemoryRented<byte>.From(message.Payload!.Value));
        await _pieceManager.AddBlockAsync(block);
        await MyBitField.HavePiece(index, cancellationToken);
    }

    private async ValueTask SendPieceAsync(CancellationToken cancellationToken) { }

    private void ReceiveCancel(Message message)
    {
        var span = message.Payload!.Value.Memory.Span;
        var index = BinaryPrimitives.ReadInt32BigEndian(span[..4]);
        var begin = BinaryPrimitives.ReadInt32BigEndian(span[4..8]);
        var length = BinaryPrimitives.ReadInt32BigEndian(span[8..12]);

        var request = new Request(index, begin, length);
        _requestManager.CancelRequest(request);
    }

    public async ValueTask PerformHandshakeAsync(
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

    private async Task SendHave(int pieceIndex, CancellationToken cancellationToken)
    {
        using var haveMessage = Message.CreateHave(pieceIndex);
        await SendMessage(haveMessage, cancellationToken);
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

    private async ValueTask<(
        IMemoryOwner<byte> pool,
        Handshake receivedHandshake
    )> ReceiveHandshake(CancellationToken cancellationToken)
    {
        using var cts = cancellationToken.WithTimeout(10.Seconds());
        using var pool = MemoryPool<byte>.Shared.Rent(Handshake.TotalLength);
        var buffer = pool.Memory[..Handshake.TotalLength];
        await Stream.ReadExactlyAsync(buffer, cts.Token);
        var receivedHandshake = Handshake.FromBytes(buffer.Span);
        return (pool, receivedHandshake);
    }

    private async ValueTask<MemoryRented<byte>> SendHandHandshake(
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
        _requestManager.Dispose();
        TcpClient.Close();
        TcpClient.Dispose();
    }
}
