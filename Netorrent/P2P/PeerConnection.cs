using System.Buffers;
using System.Buffers.Binary;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.Other;
using Netorrent.P2P.Download;
using Netorrent.P2P.Measurement;
using Netorrent.P2P.Messages;
using Netorrent.P2P.Upload;
using R3;

namespace Netorrent.P2P;

internal class PeerConnection(
    PeerEndpoint peerEndpoint,
    Bitfield myBitField,
    IUploadScheduler uploadScheduler,
    IRequestScheduler requestScheduler,
    IMessageStream messageStream,
    PeerRequestWindow peerRequestWindow,
    IPiecePicker piecePicker,
    bool amChoking = true,
    bool amInterested = false,
    bool peerChoking = true,
    bool peerInterested = false
) : IPeerConnection
{
    private readonly SynchronizedReactiveProperty<bool> _amChoking = new(amChoking);
    private readonly SynchronizedReactiveProperty<bool> _amInterested = new(amInterested);
    private readonly SynchronizedReactiveProperty<bool> _peerChoking = new(peerChoking);
    private readonly SynchronizedReactiveProperty<bool> _peerInterested = new(peerInterested);
    private readonly SynchronizedReactiveProperty<bool> _activeDownloader = new(false);

    private DateTimeOffset _lastSentMessageTime;
    private DateTimeOffset _lastReceivedMessageTime;
    private DateTimeOffset _lastSentBlock;
    private DateTimeOffset _lastReceivedBlock;
    private CancellationTokenSource? _cancellationTokenSource;
    private DateTimeOffset _startedConnectionTime;
    private Task? _runTask;
    private bool _disposed;

    public SpeedTracker DownloadTracker { get; } = new();
    public SpeedTracker UploadTracker { get; } = new();
    public Bitfield MyBitField { get; } = myBitField;
    public Bitfield? PeerBitField { get; private set; }
    public PeerEndpoint PeerEndpoint { get; } = peerEndpoint;
    public PeerRequestWindow PeerRequestWindow { get; } = peerRequestWindow;

    public TimeSpan TimeSinceReceivedBlock => DateTimeOffset.UtcNow - _lastReceivedBlock;
    public TimeSpan TimeSinceSentBlock => DateTimeOffset.UtcNow - _lastSentBlock;

    private ulong _requestedBlocksCount;
    private ulong _uploadRequestedCount;

    public ulong RequestedBlocksCount => Interlocked.Read(ref _requestedBlocksCount);
    public ulong UploadRequestedBlocksCount => Interlocked.Read(ref _uploadRequestedCount);

    public TimeSpan ConnectionDuration => DateTimeOffset.UtcNow - _startedConnectionTime;

    public ReadOnlyReactiveProperty<bool> AmChoking => _amChoking;
    public ReadOnlyReactiveProperty<bool> AmInterested => _amInterested;
    public ReadOnlyReactiveProperty<bool> PeerChoking => _peerChoking;
    public ReadOnlyReactiveProperty<bool> PeerInterested => _peerInterested;
    public ReactiveProperty<bool> ActiveDownloader => _activeDownloader;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _startedConnectionTime = DateTimeOffset.UtcNow;
        _lastSentMessageTime = DateTimeOffset.UtcNow;
        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken
        );

        _runTask = RunAsync(_cancellationTokenSource);

        return _runTask;
    }

    public void Unchoke() => _amChoking.Value = false;

    public void Choke() => _amChoking.Value = true;

    public ulong IncrementUploadRequested() => Interlocked.Increment(ref _uploadRequestedCount);

    public ulong DecrementUploadRequested() => Interlocked.Decrement(ref _uploadRequestedCount);

    public ulong IncrementRequestedBlock() => Interlocked.Increment(ref _requestedBlocksCount);

    public ulong DecrementRequestedBlock() => Interlocked.Decrement(ref _requestedBlocksCount);

    private void RegisterPiece(int index) => piecePicker.IncreaseRarity(index);

    public bool TrySendRequest(RequestBlock nextBlock)
    {
        var requestMessage = Message.CreateRequest(
            nextBlock.Index,
            nextBlock.Begin,
            nextBlock.Length
        );
        return TryWriteMessage(requestMessage);
    }

    public bool TrySendCancel(RequestBlock request)
    {
        var cancelMessage = Message.CreateCancel(request.Index, request.Begin, request.Length);
        return TryWriteMessage(cancelMessage);
    }

    public bool TrySendBlock(Block block)
    {
        var pieceMessage = Message.CreatePiece(block.Index, block.Begin, block.Payload);
        if (TryWriteMessage(pieceMessage))
        {
            UploadTracker.AddBytes(block.Payload.Length);
            _lastSentBlock = DateTimeOffset.UtcNow;
            return true;
        }
        return false;
    }

    private async Task RunAsync(CancellationTokenSource cancellationTokenSource)
    {
        using var stateChangedDisposable = MyBitField.StateChanged.SubscribeAwait(
            async (i, ct) => await SendHaveAsync(i, ct).ConfigureAwait(false),
            configureAwait: false
        );

        using var amChokingDisposable = _amChoking.SubscribeAwait(
            async (state, cancellationToken) =>
            {
                var message = state ? Message.CreateChoke() : Message.CreateUnchoke();
                await WriteMessageAsync(message, cancellationToken).ConfigureAwait(false);
            },
            AwaitOperation.Switch,
            configureAwait: false
        );

        using var amInterestedDisposable = _amInterested.SubscribeAwait(
            async (state, cancellationToken) =>
            {
                var message = state ? Message.CreateInterested() : Message.CreateNotInterested();

                await WriteMessageAsync(message, cancellationToken).ConfigureAwait(false);
                requestScheduler.TryRequest(this);
            },
            AwaitOperation.Switch,
            configureAwait: false
        );

        using var peerChokingDisposable = _peerChoking.Subscribe(
            (state) =>
            {
                requestScheduler.TryRequest(this);
            }
        );

        using var peerInterestedDisposable = _peerInterested.Subscribe(
            (state) =>
            {
                uploadScheduler.TryRunRound(this);
            }
        );

        await SendBitfieldAsync(MyBitField, cancellationTokenSource.Token).ConfigureAwait(false);

        await using var downloadTimer = DownloadTracker
            .StartSampling(100.Milliseconds)
            .ConfigureAwait(false);
        await using var uploadTimer = UploadTracker
            .StartSampling(100.Milliseconds)
            .ConfigureAwait(false);
        await using var requestWindowTimer = PeerRequestWindow
            .StartSampling(100.Milliseconds, DownloadTracker)
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

    private async Task CheckTimeoutAsync(CancellationToken cancellationToken)
    {
        var waitTime = 10.Seconds;
        var keepAliveThreshold = 1.Minutes;

        while (!cancellationToken.IsCancellationRequested)
        {
            var timePassed = DateTimeOffset.UtcNow - _lastSentMessageTime;
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
            _lastReceivedMessageTime = DateTimeOffset.UtcNow;

            if (message.Id == 255) //Keep-alive
            {
                continue;
            }

            if (message.Id == Message.Bitfield)
            {
                ReceiveBitfield(message);
                continue;
            }

            if (message.Id == Message.Interested)
            {
                ReceiveInterested();
                continue;
            }

            if (message.Id == Message.NotInterested)
            {
                ReceiveNotInterested();
                continue;
            }

            if (message.Id == Message.Choke)
            {
                ReceiveChoke();
                continue;
            }

            if (message.Id == Message.Unchoke)
            {
                ReceiveUnchoke();
                continue;
            }

            if (message.Id == Message.Have)
            {
                ReceiveHave(message);
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

    private void ReceiveBitfield(Message message)
    {
        if (PeerBitField is not null)
        {
            throw new InvalidOperationException("Second bitfield received, dropping connection");
        }

        var bitfieldBytes = message.Payload!.Memory;
        PeerBitField = new Bitfield(bitfieldBytes.Span, MyBitField.Length);
        RegisterPieces(PeerBitField);
        CheckInterest();
    }

    private void ReceiveHave(Message message)
    {
        //Lazy bitfield
        PeerBitField ??= new(MyBitField.Length);
        int pieceIndex = BinaryPrimitives.ReadInt32BigEndian(message.Payload!.Memory.Span);
        //If the have was already sent or we already know that he has that piece we omit this message
        if (PeerBitField.HasPiece(pieceIndex))
        {
            return;
        }

        RegisterPiece(pieceIndex);
        PeerBitField.SetPiece(pieceIndex);
        CheckInterest();
    }

    private void ReceiveInterested()
    {
        _peerInterested.Value = true;
    }

    private void ReceiveNotInterested()
    {
        _peerInterested.Value = false;
    }

    private void ReceiveChoke()
    {
        _peerChoking.Value = true;
    }

    private void ReceiveUnchoke()
    {
        _peerChoking.Value = false;
    }

    private async ValueTask ReceiveRequestAsync(
        Message message,
        CancellationToken cancellationToken
    )
    {
        if (_amChoking.Value)
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
        await uploadScheduler.AddRequestAsync(request, cancellationToken).ConfigureAwait(false);
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
        await requestScheduler.ReceiveBlockAsync(block, cancellationToken).ConfigureAwait(false);
        DownloadTracker.AddBytes(payloadLength);
        _lastReceivedBlock = DateTimeOffset.UtcNow;
    }

    private void ReceiveCancel(Message message)
    {
        var span = message.Payload!.Memory.Span;
        var index = BinaryPrimitives.ReadInt32BigEndian(span[..4]);
        var begin = BinaryPrimitives.ReadInt32BigEndian(span[4..8]);
        var length = BinaryPrimitives.ReadInt32BigEndian(span[8..12]);

        var request = new RequestBlock(index, begin, length);
        uploadScheduler.CancelRequest(request);
    }

    private async Task SendHaveAsync(int pieceIndex, CancellationToken cancellationToken)
    {
        CheckInterest();
        var message = Message.CreateHave(pieceIndex);
        await WriteMessageAsync(message, cancellationToken).ConfigureAwait(false);
    }

    private void CheckInterest()
    {
        if (PeerBitField is null)
        {
            return;
        }

        var interest = MyBitField.HasAnyMissingPiece(PeerBitField);
        _amInterested.Value = interest;
    }

    private async ValueTask SendBitfieldAsync(
        Bitfield bitField,
        CancellationToken cancellationToken
    )
    {
        var memoryRented = bitField.ToRentedArray();
        var message = Message.CreateBitfield(memoryRented);
        await WriteMessageAsync(message, cancellationToken).ConfigureAwait(false);
    }

    private void RegisterPieces(Bitfield bitfield)
    {
        for (int i = 0; i < bitfield.Length; i++)
        {
            if (bitfield.HasPiece(i))
            {
                piecePicker.IncreaseRarity(i);
            }
        }
    }

    private void UnregisterPieces(Bitfield bitfield)
    {
        for (int i = 0; i < bitfield.Length; i++)
        {
            if (bitfield.HasPiece(i))
            {
                piecePicker.DecreaseRarity(i);
            }
        }
    }

    private bool TryWriteMessage(Message message)
    {
        if (messageStream.OutgoingMessages.TryWriteOrDispose(message))
        {
            _lastSentMessageTime = DateTimeOffset.UtcNow;
            return true;
        }
        return false;
    }

    private async ValueTask WriteMessageAsync(Message message, CancellationToken cancellationToken)
    {
        await messageStream
            .OutgoingMessages.WriteOrDisposeAsync(message, cancellationToken)
            .ConfigureAwait(false);
        _lastSentMessageTime = DateTimeOffset.UtcNow;
    }

    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;

            if (PeerBitField is not null)
            {
                UnregisterPieces(PeerBitField);
            }

            _cancellationTokenSource?.Cancel();

            try
            {
                if (_runTask is not null)
                {
                    await _runTask.ConfigureAwait(false);
                }
            }
            catch { }

            AmChoking.Dispose();
            AmInterested.Dispose();
            PeerChoking.Dispose();
            PeerInterested.Dispose();

            _cancellationTokenSource?.Dispose();
            await messageStream.DisposeAsync().ConfigureAwait(false);
        }
    }
}
