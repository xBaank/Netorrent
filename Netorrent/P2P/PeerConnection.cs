using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Lazy;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.Other;
using Netorrent.P2P.Managers.Piece;
using Netorrent.P2P.Managers.Request;
using Netorrent.P2P.Measurement;
using Netorrent.P2P.Messages;
using TimeSpanXt;
using ZLinq;

namespace Netorrent.P2P;

internal class PeerConnection(
    TcpClient tcpClient,
    IPEndPoint iPEndPoint,
    Bitfield myBitField,
    FileManager fileManager,
    UploadScheduler uploadScheduler,
    PieceManager pieceManager,
    PieceSelector pieceSelector,
    ILogger logger,
    bool amChocking = true,
    bool amInterested = false,
    bool peerChocking = true,
    bool peerInterested = false
) : IAsyncDisposable
{
    private const int TimeoutInSeconds = 120;

    [Lazy]
    private NetworkStream Stream => TcpClient.GetStream();
    private readonly FileManager _fileManager = fileManager;
    private readonly UploadScheduler _uploadScheduler = uploadScheduler;
    private readonly PieceManager _pieceManager = pieceManager;
    private readonly PieceSelector _pieceSelector = pieceSelector;
    private readonly ILogger _logger = logger;
    private readonly Channel<Message> _incomingMessages = Channel.CreateBounded<Message>(
        new BoundedChannelOptions(50) { SingleWriter = true, SingleReader = true }
    );
    private readonly Channel<Message> _outgoingMessages = Channel.CreateBounded<Message>(
        new BoundedChannelOptions(50) { SingleWriter = false, SingleReader = true }
    );
    private readonly ConcurrentQueue<Block> _currentPieceBlocks = new();
    private DateTime _lastKeepAlive;
    private Task? _loopTask;
    private CancellationTokenSource? _cancellationTokenSource;
    private DateTime _startedConnectionTime;

    public SpeedTracker SpeedTracker { get; } = new();
    public TcpClient TcpClient { get; } = tcpClient;
    public IPEndPoint IPEndPoint { get; } = iPEndPoint;
    public Bitfield MyBitField { get; } = myBitField;
    public bool AmChocking { get; private set; } = amChocking;
    public bool AmInterested { get; private set; } = amInterested;
    public bool PeerChocking { get; private set; } = peerChocking;
    public bool PeerInterested { get; private set; } = peerInterested;
    public PeerId? PeerId { get; private set; }
    public Bitfield PeerBitField { get; private set; } = new(myBitField.Length);
    public int? CurrentPieceIndex => _pieceManager.CurrentDownloadingPieceIndex;
    public Task? WaitTask => _loopTask;
    public TimeSpan ConnectionDuration => DateTime.UtcNow - _startedConnectionTime;

    public void Start(CancellationToken cancellationToken) =>
        _loopTask ??= RunPeerLoopAsync(cancellationToken);

    private async Task RunPeerLoopAsync(CancellationToken cancellationToken)
    {
        _startedConnectionTime = DateTime.Now;
        _lastKeepAlive = DateTime.UtcNow;
        await SendBitfieldAsync(MyBitField, cancellationToken);
        MyBitField.OnHavePieceAsync += SendHave;
        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken
        );
        var faultedTask = await Task.WhenAny(
            WriteLoopAsync(_cancellationTokenSource.Token),
            ReadLoopAsync(_cancellationTokenSource.Token),
            ProcessIncomingMessagesAsync(_cancellationTokenSource.Token),
            ProcessBlocksToRequestAsync(_cancellationTokenSource.Token),
            ProcessReceivedBlocksAsync(_cancellationTokenSource.Token),
            ProcessReceivedRequestsAsync(_cancellationTokenSource.Token),
            CheckTimeout(_cancellationTokenSource.Token)
        );
        _cancellationTokenSource.Cancel();
        await faultedTask;
    }

    public async Task CheckTimeout(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var timePassed = DateTime.UtcNow - _lastKeepAlive;
            if (timePassed > 1.Minutes())
            {
                await _outgoingMessages.Writer.WriteAsync(Message.KeepAlive, cancellationToken);
            }
            await Task.Delay(10.Seconds(), cancellationToken);
        }
    }

    private async Task WriteLoopAsync(CancellationToken cancellationToken)
    {
        await foreach (var item in _outgoingMessages.Reader.ReadAllAsync(cancellationToken))
        {
            if (item.Id == Message.Request && PeerChocking)
            {
                //Try to enqueue it again and wait until UnChoke
                await _outgoingMessages.Writer.WriteAsync(item, cancellationToken);
                await Task.Delay(500, cancellationToken);
                continue;
            }

            using var message = item;
            await SendMessage(message, cancellationToken);
        }
    }

    private async Task ReadLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var message = await ReceiveMessage(cancellationToken);
            await _incomingMessages.Writer.WriteAsync(message, cancellationToken);
        }
    }

    private async Task ProcessIncomingMessagesAsync(CancellationToken cancellationToken)
    {
        await foreach (var item in _incomingMessages.Reader.ReadAllAsync(cancellationToken))
        {
            using var message = item;

            if (message.Id == 255) //Keep-alive
                continue;

            if (message.Id == Message.Bitfield)
            {
                var bitfieldBytes = message.Payload!.Value.Memory;
                PeerBitField = new Bitfield(bitfieldBytes.Span, MyBitField.Length);
                await SendInterest(cancellationToken);
                continue;
            }

            if (message.Id == Message.Interested)
            {
                PeerInterested = true;
                await SendUnchoked(cancellationToken);
                continue;
            }

            if (message.Id == Message.NotInterested)
            {
                PeerInterested = false;
                await SendChoked(cancellationToken);
                continue;
            }

            if (message.Id == Message.Choke)
            {
                PeerChocking = true;
                await _pieceManager.DiscardSentRequests(cancellationToken);
                continue;
            }

            if (message.Id == Message.Unchoke)
            {
                PeerChocking = false;
                continue;
            }

            if (message.Id == Message.Have)
            {
                int pieceIndex = BinaryPrimitives.ReadInt32BigEndian(
                    message.Payload!.Value.Memory.Span
                );
                await PeerBitField.AddPiece(pieceIndex, cancellationToken);
                await SendInterest(cancellationToken);
                continue;
            }

            if (message.Id == Message.Request)
            {
                await ReceiveRequestAsync(message, cancellationToken);
                continue;
            }

            if (message.Id == Message.Piece)
            {
                await ReceiveBlockAsync(message, cancellationToken);
                continue;
            }

            if (message.Id == Message.Cancel)
            {
                ReceiveCancel(message);
                continue;
            }

            if (message.Id == Message.Port)
            {
                //TODO Implement DHT port message handling
                continue;
            }

            throw new InvalidDataException($"Invalid id {message.Id}");
        }
    }

    private async Task ProcessReceivedRequestsAsync(CancellationToken cancellationToken)
    {
        await foreach (var item in _uploadScheduler.Requests.WithCancellation(cancellationToken))
        {
            await ProcessRequestAsync(item, cancellationToken);
        }
    }

    private async Task ProcessReceivedBlocksAsync(CancellationToken cancellationToken)
    {
        await foreach (var item in _pieceManager.BlocksToWrite.WithCancellation(cancellationToken))
        {
            await ProcessBlockAsync(item, cancellationToken);
        }
    }

    private async Task ProcessBlocksToRequestAsync(CancellationToken cancellationToken)
    {
        await foreach (
            var item in _pieceManager.BlocksToRequest.WithCancellation(cancellationToken)
        )
        {
            while (!cancellationToken.IsCancellationRequested && PeerChocking)
            {
                await Task.Delay(50, cancellationToken);
            }
            await SendRequestAsync(item, cancellationToken);
        }
    }

    public async ValueTask SetPieceToDownloadAsync(int index, CancellationToken cancellationToken)
    {
        var blocks = _fileManager.GetBlocksByPieceIndex(index);
        await _pieceManager.SetCurrentPieceAsync(index, blocks, cancellationToken);
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

        var request = new RequestBlock(index, begin, length);
        var response = await _uploadScheduler.AddRequestAsync(request, cancellationToken);

        if (response == RequestResponseType.Violation)
            throw new InvalidDataException("Received invalid request from peer.");

        if (response == RequestResponseType.Ignored)
            return;
    }

    private async Task SendRequestAsync(RequestBlock nextBlock, CancellationToken cancellationToken)
    {
        var requestMessage = Message.CreateRequest(
            nextBlock.Index,
            nextBlock.Begin,
            nextBlock.Length
        );
        await _outgoingMessages.Writer.WriteAsync(requestMessage, cancellationToken);
    }

    private async ValueTask ProcessRequestAsync(
        RequestBlock request,
        CancellationToken cancellationToken
    )
    {
        using var pieceData = await _fileManager.ReadPieceAsync(
            request.Index,
            request.Begin,
            request.Length,
            cancellationToken
        );

        var pieceMessage = Message.CreatePiece(request.Index, request.Begin, pieceData);
        await _outgoingMessages.Writer.WriteAsync(pieceMessage, cancellationToken);
    }

    private async ValueTask ReceiveBlockAsync(Message message, CancellationToken cancellationToken)
    {
        var span = message.Payload!.Value.Memory.Span;
        var index = BinaryPrimitives.ReadInt32BigEndian(span[..4]);
        var begin = BinaryPrimitives.ReadInt32BigEndian(span[4..8]);
        var array = ArrayPool<byte>.Shared.Rent(span.Length - 8);
        span[8..].CopyTo(array.AsSpan());
        var block = new Block(index, begin, new RentedArray<byte>(array, span.Length - 8));
        await _pieceManager.AddBlockAsync(block, cancellationToken);
    }

    private async ValueTask ProcessBlockAsync(Block block, CancellationToken cancellationToken)
    {
        _currentPieceBlocks.Enqueue(block);
        var memory = block.Payload.Memory;
        _pieceManager.SetBlockWritten(block);
        SpeedTracker.AddBytes(memory.Length);

        if (_pieceManager.HasFinishedCurrentPiece)
        {
            var blocksList = _currentPieceBlocks.AsValueEnumerable().OrderBy(i => i.Begin).ToList();

            if (blocksList.Count == 0)
                return;

            using var combined = blocksList.Select(i => i.Payload.Memory).ToList().Combine();

            var start = blocksList[0];
            var pieceIndex = start.Index;

            while (_currentPieceBlocks.TryDequeue(out var dequeued))
            {
                dequeued.Dispose();
            }

            await _fileManager.WritePieceAsync(
                pieceIndex,
                start.Begin,
                combined.Memory,
                cancellationToken
            );

            var isOk = await _fileManager.VerifyPieceAsync(
                _pieceManager.CurrentDownloadingPieceIndex!.Value,
                combined.Memory,
                cancellationToken
            );

            if (!isOk)
            {
                await _fileManager.ClearPieceAsync(
                    _pieceManager.CurrentDownloadingPieceIndex!.Value,
                    cancellationToken
                );
                return;
            }

            await MyBitField.AddPiece(
                _pieceManager.CurrentDownloadingPieceIndex!.Value,
                cancellationToken
            );

            _pieceManager.SetCurrentPieceAsCompleted();
            await _pieceSelector.OnPeerRequestPiece(this, cancellationToken);
        }
    }

    private void ReceiveCancel(Message message)
    {
        var span = message.Payload!.Value.Memory.Span;
        var index = BinaryPrimitives.ReadInt32BigEndian(span[..4]);
        var begin = BinaryPrimitives.ReadInt32BigEndian(span[4..8]);
        var length = BinaryPrimitives.ReadInt32BigEndian(span[8..12]);

        var request = new RequestBlock(index, begin, length);
        _uploadScheduler.CancelRequest(request);
    }

    public async ValueTask PerformHandshakeAsync(
        ReadOnlyMemory<byte> infoHash,
        PeerId peerId,
        CancellationToken cancellationToken = default
    )
    {
        using var timeoutCts = new CancellationTokenSource(TimeoutInSeconds.Seconds());
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
        PeerId peerId,
        CancellationToken cancellationToken = default
    )
    {
        using var timeoutCts = new CancellationTokenSource(TimeoutInSeconds.Seconds());
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
        using var cts = cancellationToken.WithTimeout(TimeoutInSeconds.Seconds());
        using var messageBytes = message.ToMemoryRented();
        await Stream.WriteAsync(messageBytes.Memory, cts.Token);
        await Stream.FlushAsync(cts.Token);
        _lastKeepAlive = DateTime.UtcNow;
    }

    private async Task SendHave(int pieceIndex, CancellationToken cancellationToken)
    {
        var message = Message.CreateHave(pieceIndex);
        await _outgoingMessages.Writer.WriteAsync(message, cancellationToken);
    }

    private async Task SendInterest(CancellationToken cancellationToken)
    {
        var interest = MyBitField.HasAnyMissingPiece(PeerBitField);
        if (interest != AmInterested)
        {
            var message = Message.CreateInterested();
            await _outgoingMessages.Writer.WriteAsync(message, cancellationToken);
            AmInterested = interest;
            await _pieceSelector.OnPeerRequestPiece(this, cancellationToken);
        }
    }

    private async Task SendChoked(CancellationToken cancellationToken)
    {
        if (!AmChocking)
        {
            var message = Message.CreateChoke();
            await _outgoingMessages.Writer.WriteAsync(message, cancellationToken);
            AmChocking = true;
        }
    }

    private async Task SendUnchoked(CancellationToken cancellationToken)
    {
        if (AmChocking)
        {
            var message = Message.CreateUnchoke();
            await _outgoingMessages.Writer.WriteAsync(message, cancellationToken);
            AmChocking = false;
        }
    }

    public async ValueTask<Message> ReceiveMessage(CancellationToken cancellationToken = default)
    {
        using var cts = cancellationToken.WithTimeout(TimeoutInSeconds.Seconds());
        using var lengthPool = MemoryPool<byte>.Shared.Rent(4);
        var lengthBuffer = lengthPool.Memory[..4];
        await Stream.ReadExactlyAsync(lengthBuffer, cts.Token);
        int messageLength = BinaryPrimitives.ReadInt32BigEndian(lengthBuffer.Span[..4]);
        if (messageLength == 0)
            return Message.CreateKeepAlive();
        var array = ArrayPool<byte>.Shared.Rent(lengthBuffer.Length + messageLength);
        var totalMessageBuffer = array.AsMemory()[..(lengthBuffer.Length + messageLength)];
        var messageBuffer = array.AsMemory().Slice(lengthBuffer.Length, messageLength);
        lengthBuffer.CopyTo(totalMessageBuffer);
        await Stream.ReadExactlyAsync(messageBuffer, cts.Token);
        return Message.From(array, totalMessageBuffer.Length);
    }

    public async Task SendBitfieldAsync(Bitfield bitField, CancellationToken cancellationToken)
    {
        var memoryRented = bitField.ToMemoryRented();
        var message = Message.CreateBitfield(memoryRented);
        await _outgoingMessages.Writer.WriteAsync(message, cancellationToken);
    }

    private void ValidateHandshake(ReadOnlyMemory<byte> infoHash, Handshake receivedHandshake)
    {
        if (receivedHandshake.InfoHash.AsSpan().SequenceEqual(infoHash.Span) is false)
            throw new InvalidDataException("InfoHash mismatch in handshake.");

        PeerId = new PeerId(receivedHandshake.PeerId);
    }

    private async ValueTask<(
        IMemoryOwner<byte> pool,
        Handshake receivedHandshake
    )> ReceiveHandshake(CancellationToken cancellationToken)
    {
        using var cts = cancellationToken.WithTimeout(TimeoutInSeconds.Seconds());
        using var pool = MemoryPool<byte>.Shared.Rent(Handshake.TotalLength);
        var buffer = pool.Memory[..Handshake.TotalLength];
        await Stream.ReadExactlyAsync(buffer, cts.Token);
        var receivedHandshake = Handshake.FromBytes(buffer.Span);
        return (pool, receivedHandshake);
    }

    private async ValueTask<RentedArray<byte>> SendHandHandshake(
        ReadOnlyMemory<byte> infoHash,
        PeerId peerId,
        CancellationToken cancellationToken
    )
    {
        using var cts = cancellationToken.WithTimeout(TimeoutInSeconds.Seconds());
        var handshake = Handshake.Create(infoHash.ToArray(), peerId.ToBytes());
        var bytesRented = handshake.ToBytes();
        await Stream.WriteAsync(bytesRented.Memory, cts.Token);
        await Stream.FlushAsync(cts.Token);
        return bytesRented;
    }

    public async ValueTask DisposeAsync()
    {
        _cancellationTokenSource?.Cancel();
        MyBitField.OnHavePieceAsync -= SendHave;
        _outgoingMessages.Writer.TryComplete();
        _incomingMessages.Writer.TryComplete();
        await foreach (var item in _outgoingMessages.Reader.ReadAllAsync())
        {
            item.Dispose();
        }
        await foreach (var item in _incomingMessages.Reader.ReadAllAsync())
        {
            item.Dispose();
        }
        while (_currentPieceBlocks.TryDequeue(out var item))
        {
            item.Dispose();
        }
        await _uploadScheduler.DisposeAsync();
        await _pieceManager.DisposeAsync();
        TcpClient.Close();
    }
}
