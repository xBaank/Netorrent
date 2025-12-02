using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Reactive.Subjects;
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
    IUploadScheduler uploadScheduler,
    IRequestScheduler requestScheduler,
    IMessageStream messageStream,
    bool amChocking = true,
    bool amInterested = false,
    bool peerChocking = true,
    bool peerInterested = false
) : IAsyncDisposable
{
    private readonly IUploadScheduler _uploadScheduler = uploadScheduler;
    private readonly IRequestScheduler _requestScheduler = requestScheduler;
    private readonly Subject<PeerConnection> _stateChanged = new();

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
    public IObservable<PeerConnection> StateChanged => _stateChanged;
    public PeerEndpoint PeerEndpoint => new(IPEndPoint, PeerId!.Value);

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
        await using var downloadTimer = DownloadSpeedTracker.StartSampling(500.Milliseconds);
        await using var uploadTimer = UploadSpeedTracker.StartSampling(500.Milliseconds);
        await _cancellationTokenSource.CancelOnFirstCompletionAndAwaitAllAsync([
            messageStream.StartAsync(_cancellationTokenSource.Token),
            ProcessIncomingMessagesAsync(_cancellationTokenSource.Token),
            CheckTimeoutAsync(_cancellationTokenSource.Token),
        ]);
        MyBitField.OnHavePieceAsync -= SendHaveAsync;
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
                await messageStream.OutgoingMessages.WriteOrDisposeAsync(
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
                for (int i = 0; i < PeerBitField.Length; i++)
                {
                    if (PeerBitField.HasPiece(i))
                        _requestScheduler.IncreaseRarity(i);
                }
                await SendInterestAsync(cancellationToken);
                continue;
            }

            if (message.Id == Message.Interested)
            {
                if (PeerInterested != true)
                {
                    PeerInterested = true;
                    await SendUnchokedAsync(cancellationToken);
                    _stateChanged.OnNext(this);
                }
                continue;
            }

            if (message.Id == Message.NotInterested)
            {
                if (PeerInterested != false)
                {
                    PeerInterested = false;
                    await SendChokedAsync(cancellationToken);
                    _stateChanged.OnNext(this);
                }
                continue;
            }

            if (message.Id == Message.Choke)
            {
                if (PeerChocking != true)
                {
                    PeerChocking = true;
                    _stateChanged.OnNext(this);
                }
                continue;
            }

            if (message.Id == Message.Unchoke)
            {
                if (PeerChocking != false)
                {
                    PeerChocking = false;
                    await _requestScheduler.OnPeerUnchockedAsync(this, cancellationToken);
                    _stateChanged.OnNext(this);
                }
                continue;
            }

            if (message.Id == Message.Have)
            {
                int pieceIndex = BinaryPrimitives.ReadInt32BigEndian(message.Payload!.Memory.Span);

                if (PeerBitField.HasPiece(pieceIndex))
                    continue;

                _requestScheduler.IncreaseRarity(pieceIndex);
                PeerBitField.SetPiece(pieceIndex, cancellationToken);
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
        await messageStream.OutgoingMessages.WriteOrDisposeAsync(requestMessage, cancellationToken);
    }

    public async ValueTask SendCancelAsync(
        RequestBlock request,
        CancellationToken cancellationToken
    )
    {
        var cancelMessage = Message.CreateCancel(request.Index, request.Begin, request.Length);
        await messageStream.OutgoingMessages.WriteOrDisposeAsync(cancelMessage, cancellationToken);
    }

    public async ValueTask SendBlockAsync(Block block, CancellationToken cancellationToken)
    {
        UploadSpeedTracker.AddBytes(block.Payload.Length);
        var pieceMessage = Message.CreatePiece(block.Index, block.Begin, block.Payload);
        await messageStream.OutgoingMessages.WriteOrDisposeAsync(pieceMessage, cancellationToken);
        UploadRequestedBlocksCount--;
    }

    private async ValueTask ReceiveBlockAsync(Message message, CancellationToken cancellationToken)
    {
        var span = message.Payload!.Memory.Span;

        int index = BinaryPrimitives.ReadInt32BigEndian(span[..4]);
        int begin = BinaryPrimitives.ReadInt32BigEndian(span[4..8]);
        int payloadLength = span.Length - 8;
        var rented = new RentedArray<byte>(
            ArrayPool<byte>.Shared.Rent(payloadLength),
            payloadLength
        );
        span[8..].CopyTo(rented.Memory.Span);
        var block = new Block(index, begin, rented, this);
        await _requestScheduler.ReceiveBlockAsync(block, cancellationToken);
        DownloadSpeedTracker.AddBytes(payloadLength);
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
        await messageStream.OutgoingMessages.WriteOrDisposeAsync(message, cancellationToken);
        await SendNotInterestedAsync(cancellationToken);
    }

    private async ValueTask SendInterestAsync(CancellationToken cancellationToken)
    {
        var interest = MyBitField.HasAnyMissingPiece(PeerBitField);
        if (interest != AmInterested)
        {
            AmInterested = interest;
            var message = Message.CreateInterested();
            await messageStream.OutgoingMessages.WriteOrDisposeAsync(message, cancellationToken);
            _stateChanged.OnNext(this);
        }
    }

    private async Task SendNotInterestedAsync(CancellationToken cancellationToken)
    {
        var interest = MyBitField.HasAnyMissingPiece(PeerBitField);
        if (interest != AmInterested)
        {
            AmInterested = interest;
            var message = Message.CreateNotInterested();
            await messageStream.OutgoingMessages.WriteOrDisposeAsync(message, cancellationToken);
            _stateChanged.OnNext(this);
        }
    }

    private async Task SendChokedAsync(CancellationToken cancellationToken)
    {
        if (AmChocking != true)
        {
            AmChocking = true;
            var message = Message.CreateChoke();
            await messageStream.OutgoingMessages.WriteOrDisposeAsync(message, cancellationToken);
            _uploadScheduler.RemoveChokedSlot();
            _stateChanged.OnNext(this);
        }
    }

    private async Task SendUnchokedAsync(CancellationToken cancellationToken)
    {
        if (AmChocking != false && _uploadScheduler.AddChokedSlot())
        {
            AmChocking = false;
            var message = Message.CreateUnchoke();
            await messageStream.OutgoingMessages.WriteOrDisposeAsync(message, cancellationToken);
            _stateChanged.OnNext(this);
        }
    }

    public async Task SendBitfieldAsync(Bitfield bitField, CancellationToken cancellationToken)
    {
        var memoryRented = bitField.ToRentedArray();
        var message = Message.CreateBitfield(memoryRented);
        await messageStream.OutgoingMessages.WriteOrDisposeAsync(message, cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        MyBitField.OnHavePieceAsync -= SendHaveAsync;
        for (int i = 0; i < PeerBitField.Length; i++)
        {
            if (PeerBitField.HasPiece(i))
                _requestScheduler.DecreaseRarity(i);
        }
        _uploadScheduler.RemoveChokedSlot();
        messageStream.Dispose();
        _cancellationTokenSource?.Dispose();
    }
}
