using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.Other;
using Netorrent.P2P.Download;
using Netorrent.P2P.Measurement;
using Netorrent.P2P.Messages;
using Netorrent.P2P.Upload;
using ZLinq;

namespace Netorrent.P2P;

internal class PeerConnection(
    TcpClient tcpClient,
    IPEndPoint iPEndPoint,
    Bitfield myBitField,
    UploadScheduler uploadScheduler,
    RequestManager requestManager,
    bool amChocking = true,
    bool amInterested = false,
    bool peerChocking = true,
    bool peerInterested = false
) : IAsyncDisposable
{
    private const int TimeoutInSeconds = 120;

    private readonly NetworkStream Stream = tcpClient.GetStream();
    private readonly UploadScheduler _uploadScheduler = uploadScheduler;
    private readonly RequestManager _requestManager = requestManager;
    private readonly Channel<Message> _incomingMessages = Channel.CreateBounded<Message>(
        new BoundedChannelOptions(256) { SingleWriter = true, SingleReader = true }
    );
    private readonly Channel<Message> _outgoingMessages = Channel.CreateBounded<Message>(
        new BoundedChannelOptions(256) { SingleWriter = false, SingleReader = true }
    );

    private DateTime _lastKeepAlive;
    private Task? _loopTask;
    private CancellationTokenSource? _cancellationTokenSource;
    private DateTime _startedConnectionTime;

    public SpeedTracker DownloadSpeedTracker { get; } = new();
    public SpeedTracker UploadSpeedTracker { get; } = new();
    public TcpClient TcpClient { get; } = tcpClient;
    public IPEndPoint IPEndPoint { get; } = iPEndPoint;
    public Bitfield MyBitField { get; } = myBitField;
    public bool AmChocking { get; private set; } = amChocking;
    public bool AmInterested { get; private set; } = amInterested;
    public bool PeerChocking { get; private set; } = peerChocking;
    public bool PeerInterested { get; private set; } = peerInterested;
    public PeerId? PeerId { get; private set; }
    public Bitfield PeerBitField { get; private set; } = new(myBitField.Length);
    public PeerRequestWindow PeerRequestWindow { get; } = new(FileManager.BlockSize);

    public int RequestedBlocksCount
    {
        get => field;
        set => Interlocked.Exchange(ref field, value);
    }
    public int UploadRequestedBlocksCount
    {
        get => field;
        set => Interlocked.Exchange(ref field, value);
    }
    public Task? WaitTask => _loopTask;
    public TimeSpan ConnectionDuration => DateTime.UtcNow - _startedConnectionTime;

    public void Start(CancellationToken cancellationToken) =>
        _loopTask ??= RunPeerLoopAsync(cancellationToken);

    private async Task RunPeerLoopAsync(CancellationToken cancellationToken)
    {
        _startedConnectionTime = DateTime.Now;
        _lastKeepAlive = DateTime.UtcNow;
        await SendBitfieldAsync(MyBitField, cancellationToken);
        MyBitField.OnHavePieceAsync += SendHaveAsync;
        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken
        );
        var faultedTask = await Task.WhenAny(
            WriteLoopAsync(_cancellationTokenSource.Token),
            ReadLoopAsync(_cancellationTokenSource.Token),
            ProcessIncomingMessagesAsync(_cancellationTokenSource.Token),
            CheckTimeoutAsync(_cancellationTokenSource.Token),
            TrackSpeedAsync(_cancellationTokenSource.Token)
        );
        _cancellationTokenSource.Cancel();
        await faultedTask;
    }

    public async Task TrackSpeedAsync(CancellationToken cancellationToken)
    {
        var waitTime = 250.Milliseconds;
        while (!cancellationToken.IsCancellationRequested)
        {
            DownloadSpeedTracker.Sample();
            UploadSpeedTracker.Sample();
            await Task.Delay(waitTime, cancellationToken);
        }
    }

    public async Task CheckTimeoutAsync(CancellationToken cancellationToken)
    {
        var waitTime = 10.Seconds;
        var keepAliveThreshold = 1.Minutes;

        while (!cancellationToken.IsCancellationRequested)
        {
            var timePassed = DateTime.UtcNow - _lastKeepAlive;
            if (timePassed > keepAliveThreshold)
            {
                await _outgoingMessages.Writer.WriteAsync(Message.KeepAlive, cancellationToken);
            }
            await Task.Delay(waitTime, cancellationToken);
        }
    }

    private async Task WriteLoopAsync(CancellationToken cancellationToken)
    {
        await foreach (var item in _outgoingMessages.Reader.ReadAllAsync(cancellationToken))
        {
            using var message = item;
            await SendMessageAsync(message, cancellationToken);
        }
    }

    private async Task ReadLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var message = await ReceiveMessageAsync(cancellationToken);
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
                var bitfieldBytes = message.Payload!.Memory;
                PeerBitField = new Bitfield(bitfieldBytes.Span, MyBitField.Length);
                foreach (var (index, hasPiece) in PeerBitField.Pieces.AsValueEnumerable().Index())
                {
                    if (hasPiece)
                        _requestManager.IncreaseRarity(index);
                }
                await SendInterestAsync(cancellationToken);
                continue;
            }

            if (message.Id == Message.Interested)
            {
                PeerInterested = true;
                await SendUnchokedAsync(cancellationToken);
                continue;
            }

            if (message.Id == Message.NotInterested)
            {
                PeerInterested = false;
                await SendChokedAsync(cancellationToken);
                continue;
            }

            if (message.Id == Message.Choke)
            {
                PeerChocking = true;
                continue;
            }

            if (message.Id == Message.Unchoke)
            {
                PeerChocking = false;
                await _requestManager.OnPeerUnchockedAsync(this, cancellationToken);
                continue;
            }

            if (message.Id == Message.Have)
            {
                int pieceIndex = BinaryPrimitives.ReadInt32BigEndian(message.Payload!.Memory.Span);

                if (PeerBitField.HasPiece(pieceIndex))
                    continue;

                _requestManager.IncreaseRarity(pieceIndex);
                await PeerBitField.SetPieceAsync(pieceIndex, cancellationToken);
                await SendInterestAsync(cancellationToken);
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

    private async ValueTask ReceiveRequestAsync(
        Message message,
        CancellationToken cancellationToken
    )
    {
        if (AmChocking)
            return;

        var span = message.Payload!.Memory.Span;
        var index = BinaryPrimitives.ReadInt32BigEndian(span[..4]);
        var begin = BinaryPrimitives.ReadInt32BigEndian(span[4..8]);
        var length = BinaryPrimitives.ReadInt32BigEndian(span[8..12]);

        var request = new RequestBlock(index, begin, length)
        {
            RequestedAt = DateTimeOffset.UtcNow,
        };
        request.RequestedFrom.Add(this);

        if (await _uploadScheduler.AddRequestAsync(request, cancellationToken))
            UploadRequestedBlocksCount++;
    }

    public async Task SendRequestAsync(RequestBlock nextBlock, CancellationToken cancellationToken)
    {
        var requestMessage = Message.CreateRequest(
            nextBlock.Index,
            nextBlock.Begin,
            nextBlock.Length
        );
        await _outgoingMessages.Writer.WriteAsync(requestMessage, cancellationToken);
    }

    public async Task SendCancelAsync(RequestBlock request, CancellationToken cancellationToken)
    {
        var cancelMessage = Message.CreateCancel(request.Index, request.Begin, request.Length);
        await _outgoingMessages.Writer.WriteAsync(cancelMessage, cancellationToken);
    }

    public async ValueTask SendBlockAsync(Block block, CancellationToken cancellationToken)
    {
        UploadSpeedTracker.AddBytes(block.Payload.Length);
        var pieceMessage = Message.CreatePiece(block.Index, block.Begin, block.Payload);
        await _outgoingMessages.Writer.WriteAsync(pieceMessage, cancellationToken);
        UploadRequestedBlocksCount--;
    }

    private async ValueTask ReceiveBlockAsync(Message message, CancellationToken cancellationToken)
    {
        var span = message.Payload!.Memory.Span;
        var index = BinaryPrimitives.ReadInt32BigEndian(span[..4]);
        var begin = BinaryPrimitives.ReadInt32BigEndian(span[4..8]);
        var array = ArrayPool<byte>.Shared.Rent(span.Length - 8);
        span[8..].CopyTo(array.AsSpan());
        var block = new Block(index, begin, new RentedArray<byte>(array, span.Length - 8), this);

        var memory = block.Payload.Memory;
        DownloadSpeedTracker.AddBytes(memory.Length);
        await _requestManager.ReceiveBlockAsync(block, this, cancellationToken);
    }

    private void ReceiveCancel(Message message)
    {
        var span = message.Payload!.Memory.Span;
        var index = BinaryPrimitives.ReadInt32BigEndian(span[..4]);
        var begin = BinaryPrimitives.ReadInt32BigEndian(span[4..8]);
        var length = BinaryPrimitives.ReadInt32BigEndian(span[8..12]);

        var request = new RequestBlock(index, begin, length);
        _uploadScheduler.CancelRequest(request);
        UploadRequestedBlocksCount--;
    }

    public async ValueTask PerformHandshakeAsync(
        ReadOnlyMemory<byte> infoHash,
        PeerId peerId,
        CancellationToken cancellationToken = default
    )
    {
        using var timeoutCts = new CancellationTokenSource(TimeoutInSeconds.Seconds);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            timeoutCts.Token
        );
        using var bytesRented = await SendHandHandshake(infoHash, peerId, linkedCts.Token);
        var (pool, receivedHandshake) = await ReceiveHandshakeAsync(linkedCts.Token);

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
        using var timeoutCts = new CancellationTokenSource(TimeoutInSeconds.Seconds);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            timeoutCts.Token
        );

        var (pool, receivedHandshake) = await ReceiveHandshakeAsync(linkedCts.Token);

        using (pool)
        {
            using var bytesRented = await SendHandHandshake(infoHash, peerId, linkedCts.Token);
            ValidateHandshake(infoHash, receivedHandshake);
        }
    }

    public async ValueTask SendMessageAsync(
        Message message,
        CancellationToken cancellationToken = default
    )
    {
        using var cts = cancellationToken.WithTimeout(TimeoutInSeconds.Seconds);
        using var messageBytes = message.ToMemoryRented();
        await Stream.WriteAsync(messageBytes.Memory, cts.Token);
        await Stream.FlushAsync(cts.Token);
        _lastKeepAlive = DateTime.UtcNow;
    }

    private async Task SendHaveAsync(int pieceIndex, CancellationToken cancellationToken)
    {
        var message = Message.CreateHave(pieceIndex);
        await _outgoingMessages.Writer.WriteAsync(message, cancellationToken);

        await SendNotInterestedAsync(cancellationToken);
    }

    private async Task SendInterestAsync(CancellationToken cancellationToken)
    {
        var interest = MyBitField.HasAnyMissingPiece(PeerBitField);
        if (interest != AmInterested)
        {
            var message = Message.CreateInterested();
            await _outgoingMessages.Writer.WriteAsync(message, cancellationToken);
            AmInterested = interest;
        }
    }

    private async Task SendNotInterestedAsync(CancellationToken cancellationToken)
    {
        var interest = MyBitField.HasAnyMissingPiece(PeerBitField);
        if (interest != AmInterested)
        {
            var message = Message.CreateNotInterested();
            await _outgoingMessages.Writer.WriteAsync(message, cancellationToken);
            AmInterested = interest;
        }
    }

    private async Task SendChokedAsync(CancellationToken cancellationToken)
    {
        if (!AmChocking)
        {
            var message = Message.CreateChoke();
            await _outgoingMessages.Writer.WriteAsync(message, cancellationToken);
            AmChocking = true;
            _uploadScheduler.RemoveChokedSlot();
        }
    }

    private async Task SendUnchokedAsync(CancellationToken cancellationToken)
    {
        if (AmChocking && _uploadScheduler.AddChokedSlot())
        {
            var message = Message.CreateUnchoke();
            await _outgoingMessages.Writer.WriteAsync(message, cancellationToken);
            AmChocking = false;
        }
    }

    public async ValueTask<Message> ReceiveMessageAsync(
        CancellationToken cancellationToken = default
    )
    {
        using var cts = cancellationToken.WithTimeout(TimeoutInSeconds.Seconds);
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
    )> ReceiveHandshakeAsync(CancellationToken cancellationToken)
    {
        using var cts = cancellationToken.WithTimeout(TimeoutInSeconds.Seconds);
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
        using var cts = cancellationToken.WithTimeout(TimeoutInSeconds.Seconds);
        var handshake = Handshake.Create(infoHash.ToArray(), peerId.ToBytes());
        var bytesRented = handshake.ToBytes();
        await Stream.WriteAsync(bytesRented.Memory, cts.Token);
        await Stream.FlushAsync(cts.Token);
        return bytesRented;
    }

    public async ValueTask DisposeAsync()
    {
        _cancellationTokenSource?.Cancel();
        MyBitField.OnHavePieceAsync -= SendHaveAsync;
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
        foreach (var (index, hasPiece) in PeerBitField.Pieces.AsValueEnumerable().Index())
        {
            if (hasPiece)
                _requestManager.DecreaseRarity(index);
        }
        _uploadScheduler.RemoveChokedSlot();
        TcpClient.Close();
    }
}
