using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.Other;
using Netorrent.P2P.Download;
using Netorrent.P2P.Measurement;
using Netorrent.P2P.Messages;
using Netorrent.P2P.Upload;

namespace Netorrent.P2P;

internal class PeerConnection(
    PeerEndpoint peerEndpoint,
    Bitfield myBitField,
    IUploadScheduler uploadScheduler,
    IRequestScheduler requestScheduler,
    IMessageStream messageStream,
    PeerRequestWindow peerRequestWindow,
    bool amChoking = true,
    bool amInterested = false,
    bool peerChoking = true,
    bool peerInterested = false
) : IAsyncDisposable
{
    private readonly IUploadScheduler _uploadScheduler = uploadScheduler;
    private readonly IRequestScheduler _requestScheduler = requestScheduler;
    private readonly Subject<PeerConnection> _stateChanged = new();
    private readonly SemaphoreSlim _stateSemaphoreSlim = new(1);
    private DateTimeOffset _lastKeepAlive;
    private CancellationTokenSource? _cancellationTokenSource;
    private DateTimeOffset _startedConnectionTime;
    private Task? _runTask;
    private bool _disposed;

    public SpeedTracker DownloadSpeedTracker { get; } = new();
    public SpeedTracker UploadSpeedTracker { get; } = new();
    public Bitfield MyBitField { get; } = myBitField;
    public bool AmChoking { get; private set; } = amChoking;
    public bool AmInterested { get; private set; } = amInterested;
    public bool PeerChoking { get; private set; } = peerChoking;
    public bool PeerInterested { get; private set; } = peerInterested;
    public Bitfield? PeerBitField { get; private set; }
    public PeerEndpoint PeerEndpoint { get; } = peerEndpoint;
    public PeerRequestWindow PeerRequestWindow { get; } = peerRequestWindow;
    public IObservable<PeerConnection> StateChanged => _stateChanged;

    private int _requestedBlocksCount;
    private int _uploadRequestedCount;

    public int RequestedBlocksCount => Volatile.Read(ref _requestedBlocksCount);
    public int UploadRequestedBlocksCount => Volatile.Read(ref _uploadRequestedCount);

    public TimeSpan ConnectionDuration => DateTime.UtcNow - _startedConnectionTime;

    public static async ValueTask<PeerConnection> CreatePeerConnectionAsync(
        Bitfield myBitField,
        IUploadScheduler uploadScheduler,
        IRequestScheduler requestScheduler,
        IMessageStream messageStream,
        PeerRequestWindow peerRequestWindow,
        bool amInitiating,
        ReadOnlyMemory<byte> infoHash,
        PeerId myPeerId,
        IPEndPoint peerEndPoint,
        CancellationToken cancellationToken
    )
    {
        PeerId peerId;

        if (amInitiating)
        {
            peerId = await messageStream
                .PerformHandshakeAsync(infoHash, myPeerId, cancellationToken)
                .ConfigureAwait(false);
        }
        else
        {
            peerId = await messageStream
                .ReceiveHandshakeAsync(infoHash, myPeerId, cancellationToken)
                .ConfigureAwait(false);
        }

        return new PeerConnection(
            new PeerEndpoint(peerEndPoint, peerId),
            myBitField,
            uploadScheduler,
            requestScheduler,
            messageStream,
            peerRequestWindow
        );
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _startedConnectionTime = DateTimeOffset.UtcNow;
        _lastKeepAlive = DateTimeOffset.UtcNow;
        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken
        );

        _runTask = RunAsync(_cancellationTokenSource);

        return _runTask;
    }

    private async Task RunAsync(CancellationTokenSource cancellationTokenSource)
    {
        await SendBitfieldAsync(MyBitField, cancellationTokenSource.Token).ConfigureAwait(false);

        using var stateChangedDisposable = MyBitField
            .StateChanged.SelectMany(i =>
                Observable.FromAsync(() => SendHaveAsync(i, cancellationTokenSource.Token))
            )
            .Subscribe();

        await using var downloadTimer = DownloadSpeedTracker
            .StartSampling(500.Milliseconds)
            .ConfigureAwait(false);
        await using var uploadTimer = UploadSpeedTracker
            .StartSampling(500.Milliseconds)
            .ConfigureAwait(false);
        await using var requestWindowTimer = PeerRequestWindow
            .StartSampling(500.Milliseconds, DownloadSpeedTracker)
            .ConfigureAwait(false);

        try
        {
            await cancellationTokenSource
                .CancelOnFirstCompletionAndAwaitAllAsync([
                    messageStream.StartAsync(cancellationTokenSource.Token),
                    ProcessIncomingMessagesAsync(cancellationTokenSource.Token),
                    CheckTimeoutAsync(cancellationTokenSource.Token),
                ])
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) { }
    }

    public async Task CheckTimeoutAsync(CancellationToken cancellationToken)
    {
        var waitTime = 10.Seconds;
        var keepAliveThreshold = 1.Minutes;

        while (!cancellationToken.IsCancellationRequested)
        {
            var timePassed = DateTimeOffset.UtcNow - _lastKeepAlive;
            if (timePassed > keepAliveThreshold)
            {
                await WriteMessageAsync(Message.KeepAlive, cancellationToken).ConfigureAwait(false);
            }
            await Task.Delay(waitTime, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task ProcessIncomingMessagesAsync(CancellationToken cancellationToken)
    {
        await foreach (
            var item in messageStream
                .IncomingMessages.ReadAllAsync(cancellationToken)
                .ConfigureAwait(false)
        )
        {
            using var message = item;

            if (message.Id == 255) //Keep-alive
                continue;

            if (message.Id == Message.Bitfield)
            {
                await ReceiveBitfieldAsync(message, cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (message.Id == Message.Interested)
            {
                await ReceiveInterestedAsync(cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (message.Id == Message.NotInterested)
            {
                await ReceiveNotInterestedAsync(cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (message.Id == Message.Choke)
            {
                await ReceiveChokeAsync(cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (message.Id == Message.Unchoke)
            {
                await ReceiveUnchokeAsync(cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (message.Id == Message.Have)
            {
                await ReceiveHaveAsync(message, cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (message.Id == Message.Request)
            {
                await ReceiveRequestAsync(message, cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (message.Id == Message.Piece)
            {
                await ReceiveBlockAsync(message, cancellationToken).ConfigureAwait(false);
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

    private async ValueTask ReceiveBitfieldAsync(
        Message message,
        CancellationToken cancellationToken
    )
    {
        if (PeerBitField is not null)
            throw new InvalidOperationException("Second bitfield received, dropping connection");

        var bitfieldBytes = message.Payload!.Memory;
        PeerBitField = new Bitfield(bitfieldBytes.Span, MyBitField.Length);
        RegisterPieces(PeerBitField);
        await CheckInterestAsync(cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask ReceiveInterestedAsync(CancellationToken cancellationToken)
    {
        if (!PeerInterested)
        {
            PeerInterested = true;
            await _uploadScheduler.RequestSlotAsync(this, cancellationToken).ConfigureAwait(false);
            _stateChanged.OnNext(this);
        }
    }

    private async ValueTask ReceiveNotInterestedAsync(CancellationToken cancellationToken)
    {
        if (PeerInterested)
        {
            PeerInterested = false;
            await SendChokedAsync(cancellationToken).ConfigureAwait(false);
            _stateChanged.OnNext(this);
        }
    }

    private async ValueTask ReceiveChokeAsync(CancellationToken cancellationToken)
    {
        if (!PeerChoking)
        {
            PeerChoking = true;
            await _requestScheduler.FreeSlotAsync(this, cancellationToken).ConfigureAwait(false);
            _stateChanged.OnNext(this);
        }
    }

    private async ValueTask ReceiveHaveAsync(Message message, CancellationToken cancellationToken)
    {
        //Lazy bitfield
        PeerBitField ??= new(MyBitField.Length);
        int pieceIndex = BinaryPrimitives.ReadInt32BigEndian(message.Payload!.Memory.Span);
        //If the have was already sent or we already know that he has that piece we omit this message
        if (PeerBitField.HasPiece(pieceIndex))
            return;

        RegisterPiece(pieceIndex);
        PeerBitField.SetPiece(pieceIndex);
        await CheckInterestAsync(cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask ReceiveUnchokeAsync(CancellationToken cancellationToken)
    {
        if (PeerChoking)
        {
            PeerChoking = false;
            await _requestScheduler.RequestSlotAsync(this, cancellationToken).ConfigureAwait(false);
            _stateChanged.OnNext(this);
        }
    }

    private async ValueTask ReceiveRequestAsync(
        Message message,
        CancellationToken cancellationToken
    )
    {
        if (AmChoking)
        {
            return;
        }

        var span = message.Payload!.Memory.Span;
        var index = BinaryPrimitives.ReadInt32BigEndian(span[..4]);
        var begin = BinaryPrimitives.ReadInt32BigEndian(span[4..8]);
        var length = BinaryPrimitives.ReadInt32BigEndian(span[8..12]);

        var request = new RequestBlock(index, begin, length)
        {
            RequestedAt = DateTimeOffset.UtcNow,
            RequestedFrom = [this],
        };
        await _uploadScheduler.AddRequestAsync(request, cancellationToken).ConfigureAwait(false);
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
        await WriteMessageAsync(requestMessage, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask SendCancelAsync(
        RequestBlock request,
        CancellationToken cancellationToken
    )
    {
        var cancelMessage = Message.CreateCancel(request.Index, request.Begin, request.Length);
        await WriteMessageAsync(cancelMessage, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask SendBlockAsync(Block block, CancellationToken cancellationToken)
    {
        var pieceMessage = Message.CreatePiece(block.Index, block.Begin, block.Payload);
        await WriteMessageAsync(pieceMessage, cancellationToken).ConfigureAwait(false);
        UploadSpeedTracker.AddBytes(block.Payload.Length);
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
        await _requestScheduler.ReceiveBlockAsync(block, cancellationToken).ConfigureAwait(false);
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
    }

    private async Task SendHaveAsync(int pieceIndex, CancellationToken cancellationToken)
    {
        var message = Message.CreateHave(pieceIndex);
        await WriteMessageAsync(message, cancellationToken).ConfigureAwait(false);
        await CheckInterestAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task CheckInterestAsync(CancellationToken cancellationToken)
    {
        await _stateSemaphoreSlim.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (PeerBitField is null)
                return;

            var interest = MyBitField.HasAnyMissingPiece(PeerBitField);
            if (interest != AmInterested)
            {
                AmInterested = interest;
                if (!interest)
                {
                    var message = Message.CreateNotInterested();
                    await WriteMessageAsync(message, cancellationToken).ConfigureAwait(false);
                    await _requestScheduler
                        .FreeSlotAsync(this, cancellationToken)
                        .ConfigureAwait(false);
                }
                if (interest)
                {
                    AmInterested = interest;
                    var message = Message.CreateInterested();
                    await WriteMessageAsync(message, cancellationToken).ConfigureAwait(false);
                }
                _stateChanged.OnNext(this);
            }
        }
        finally
        {
            _stateSemaphoreSlim.Release();
        }
    }

    private async Task SendChokedAsync(CancellationToken cancellationToken)
    {
        if (AmChoking != true)
        {
            AmChoking = true;
            var message = Message.CreateChoke();
            await WriteMessageAsync(message, cancellationToken).ConfigureAwait(false);
            await _uploadScheduler.FreeSlotAsync(this, cancellationToken).ConfigureAwait(false);
            _stateChanged.OnNext(this);
        }
    }

    public async Task SendUnchokedAsync(CancellationToken cancellationToken)
    {
        if (AmChoking != false)
        {
            AmChoking = false;
            var message = Message.CreateUnchoke();
            await WriteMessageAsync(message, cancellationToken).ConfigureAwait(false);
            _stateChanged.OnNext(this);
        }
    }

    private async Task SendBitfieldAsync(Bitfield bitField, CancellationToken cancellationToken)
    {
        var memoryRented = bitField.ToRentedArray();
        var message = Message.CreateBitfield(memoryRented);
        await WriteMessageAsync(message, cancellationToken).ConfigureAwait(false);
    }

    public int IncrementUploadRequested() => Interlocked.Increment(ref _uploadRequestedCount);

    public int DecrementUploadRequested() => Interlocked.Decrement(ref _uploadRequestedCount);

    public int IncrementRequestedBlock() => Interlocked.Increment(ref _requestedBlocksCount);

    public int DecrementRequestedBlock() => Interlocked.Decrement(ref _requestedBlocksCount);

    private void RegisterPiece(int index) => _requestScheduler.IncreaseRarity(index);

    private void RegisterPieces(Bitfield bitfield)
    {
        for (int i = 0; i < bitfield.Length; i++)
        {
            if (bitfield.HasPiece(i))
                _requestScheduler.IncreaseRarity(i);
        }
    }

    private void UnregisterPieces(Bitfield bitfield)
    {
        for (int i = 0; i < bitfield.Length; i++)
        {
            if (bitfield.HasPiece(i))
                _requestScheduler.DecreaseRarity(i);
        }
    }

    private async ValueTask WriteMessageAsync(Message message, CancellationToken cancellationToken)
    {
        await messageStream
            .OutgoingMessages.WriteOrDisposeAsync(message, cancellationToken)
            .ConfigureAwait(false);
        _lastKeepAlive = DateTimeOffset.UtcNow;
    }

    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;

            if (PeerBitField is not null)
                UnregisterPieces(PeerBitField);

            await _uploadScheduler.FreeSlotAsync(this, default).ConfigureAwait(false);
            await _requestScheduler.FreeSlotAsync(this, default).ConfigureAwait(false);

            _cancellationTokenSource?.Cancel();

            try
            {
                if (_runTask is not null)
                    await _runTask.ConfigureAwait(false);
            }
            catch { }

            _cancellationTokenSource?.Dispose();
            await messageStream.DisposeAsync().ConfigureAwait(false);
            _stateSemaphoreSlim.Dispose();
        }
    }
}
