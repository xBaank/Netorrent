using System.Buffers;
using System.Buffers.Binary;
using System.Net;
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
    IPEndPoint iPEndPoint,
    Bitfield myBitField,
    UploadScheduler uploadScheduler,
    RequestManager requestManager,
    MessageStream messageStream,
    bool amChocking = true,
    bool amInterested = false,
    bool peerChocking = true,
    bool peerInterested = false
) : IAsyncDisposable
{
    private readonly UploadScheduler _uploadScheduler = uploadScheduler;
    private readonly RequestManager _requestManager = requestManager;

    private DateTime _lastKeepAlive;
    private CancellationTokenSource? _cancellationTokenSource;
    private DateTime _startedConnectionTime;

    public SpeedTracker DownloadSpeedTracker { get; } = new();
    public SpeedTracker UploadSpeedTracker { get; } = new();
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
    public TimeSpan ConnectionDuration => DateTime.UtcNow - _startedConnectionTime;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _startedConnectionTime = DateTime.Now;
        _lastKeepAlive = DateTime.UtcNow;
        await SendBitfieldAsync(MyBitField, cancellationToken);
        MyBitField.OnHavePieceAsync += SendHaveAsync;
        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken
        );
        var finishedTask = await Task.WhenAny(
            messageStream.StartAsync(_cancellationTokenSource.Token),
            ProcessIncomingMessagesAsync(_cancellationTokenSource.Token),
            CheckTimeoutAsync(_cancellationTokenSource.Token),
            TrackSpeedAsync(_cancellationTokenSource.Token)
        );
        if (finishedTask.IsFaulted)
        {
            _cancellationTokenSource.Cancel();
        }
        await finishedTask;
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
                await messageStream.OutgoingMessages.WriteAsync(
                    Message.KeepAlive,
                    cancellationToken
                );
            }
            await Task.Delay(waitTime, cancellationToken);
        }
    }

    private async Task ProcessIncomingMessagesAsync(CancellationToken cancellationToken)
    {
        await foreach (var item in messageStream.IncomingMessages.ReadAllAsync(cancellationToken))
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

    public async ValueTask SendRequestAsync(
        RequestBlock nextBlock,
        CancellationToken cancellationToken
    )
    {
        var requestMessage = Message.CreateRequest(
            nextBlock.Index,
            nextBlock.Begin,
            nextBlock.Length
        );
        await messageStream.OutgoingMessages.WriteAsync(requestMessage, cancellationToken);
    }

    public async ValueTask SendCancelAsync(
        RequestBlock request,
        CancellationToken cancellationToken
    )
    {
        var cancelMessage = Message.CreateCancel(request.Index, request.Begin, request.Length);
        await messageStream.OutgoingMessages.WriteAsync(cancelMessage, cancellationToken);
    }

    public async ValueTask SendBlockAsync(Block block, CancellationToken cancellationToken)
    {
        UploadSpeedTracker.AddBytes(block.Payload.Length);
        var pieceMessage = Message.CreatePiece(block.Index, block.Begin, block.Payload);
        await messageStream.OutgoingMessages.WriteAsync(pieceMessage, cancellationToken);
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
        await _requestManager.ReceiveBlockAsync(block, cancellationToken);
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
    ) => PeerId = await messageStream.PerformHandshakeAsync(infoHash, peerId, cancellationToken);

    public async ValueTask ReceiveHandshakeAsync(
        ReadOnlyMemory<byte> infoHash,
        PeerId peerId,
        CancellationToken cancellationToken = default
    ) => PeerId = await messageStream.ReceiveHandshakeAsync(infoHash, peerId, cancellationToken);

    private async Task SendHaveAsync(int pieceIndex, CancellationToken cancellationToken)
    {
        var message = Message.CreateHave(pieceIndex);
        await messageStream.OutgoingMessages.WriteAsync(message, cancellationToken);

        await SendNotInterestedAsync(cancellationToken);
    }

    private async ValueTask SendInterestAsync(CancellationToken cancellationToken)
    {
        var interest = MyBitField.HasAnyMissingPiece(PeerBitField);
        if (interest != AmInterested)
        {
            var message = Message.CreateInterested();
            await messageStream.OutgoingMessages.WriteAsync(message, cancellationToken);
            AmInterested = interest;
        }
    }

    private async Task SendNotInterestedAsync(CancellationToken cancellationToken)
    {
        var interest = MyBitField.HasAnyMissingPiece(PeerBitField);
        if (interest != AmInterested)
        {
            var message = Message.CreateNotInterested();
            await messageStream.OutgoingMessages.WriteAsync(message, cancellationToken);
            AmInterested = interest;
        }
    }

    private async Task SendChokedAsync(CancellationToken cancellationToken)
    {
        if (!AmChocking)
        {
            var message = Message.CreateChoke();
            await messageStream.OutgoingMessages.WriteAsync(message, cancellationToken);
            AmChocking = true;
            _uploadScheduler.RemoveChokedSlot();
        }
    }

    private async Task SendUnchokedAsync(CancellationToken cancellationToken)
    {
        if (AmChocking && _uploadScheduler.AddChokedSlot())
        {
            var message = Message.CreateUnchoke();
            await messageStream.OutgoingMessages.WriteAsync(message, cancellationToken);
            AmChocking = false;
        }
    }

    public async Task SendBitfieldAsync(Bitfield bitField, CancellationToken cancellationToken)
    {
        var memoryRented = bitField.ToMemoryRented();
        var message = Message.CreateBitfield(memoryRented);
        await messageStream.OutgoingMessages.WriteAsync(message, cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        _cancellationTokenSource?.Cancel();
        MyBitField.OnHavePieceAsync -= SendHaveAsync;
        foreach (var (index, hasPiece) in PeerBitField.Pieces.AsValueEnumerable().Index())
        {
            if (hasPiece)
                _requestManager.DecreaseRarity(index);
        }
        _uploadScheduler.RemoveChokedSlot();
    }
}
