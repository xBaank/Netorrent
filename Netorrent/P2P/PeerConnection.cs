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
        return TryWriteMessage(
            new Message.RequestBlockMessage(nextBlock.Index, nextBlock.Begin, nextBlock.Length)
        );
    }

    public bool TrySendCancel(RequestBlock request)
    {
        return TryWriteMessage(
            new Message.CancelMessage(request.Index, request.Begin, request.Length)
        );
    }

    public bool TrySendBlock(Block block)
    {
        if (TryWriteMessage(new Message.BlockMessage(block.Index, block.Begin, block.Payload)))
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
                Message message = state ? Message.Choke.Value : Message.Unchoke.Value;
                await WriteMessageAsync(message, cancellationToken).ConfigureAwait(false);
            },
            AwaitOperation.Switch,
            configureAwait: false
        );

        using var amInterestedDisposable = _amInterested.SubscribeAwait(
            async (state, cancellationToken) =>
            {
                Message message = state ? Message.Interested.Value : Message.NotInterested.Value;

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
                    messageStream.StartAsync(ProcessMessageAsync, cancellationTokenSource.Token),
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
                await WriteMessageAsync(Message.KeepAlive.Value, cancellationToken)
                    .ConfigureAwait(false);
            }

            await Task.Delay(waitTime, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask ProcessMessageAsync(
        Message message,
        CancellationToken cancellationToken
    )
    {
        _lastReceivedMessageTime = DateTimeOffset.UtcNow;

        if (message is Message.KeepAlive) //Keep-alive
        {
            return;
        }

        if (message is Message.BitfieldMessage bitfield)
        {
            ReceiveBitfield(bitfield);
            return;
        }

        if (message is Message.Interested)
        {
            ReceiveInterested();
            return;
        }

        if (message is Message.NotInterested)
        {
            ReceiveNotInterested();
            return;
        }

        if (message is Message.Choke)
        {
            ReceiveChoke();
            return;
        }

        if (message is Message.Unchoke)
        {
            ReceiveUnchoke();
            return;
        }

        if (message is Message.Have have)
        {
            ReceiveHave(have);
            return;
        }

        if (message is Message.RequestBlockMessage request)
        {
            await ReceiveRequestAsync(request, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (message is Message.BlockMessage block)
        {
            await ReceiveBlockAsync(block, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (message is Message.CancelMessage cancel)
        {
            ReceiveCancel(cancel);
            return;
        }

        if (message is Message.Port)
        {
            //TODO Implement DHT port message handling
            return;
        }
    }

    private void ReceiveBitfield(Message.BitfieldMessage message)
    {
        if (PeerBitField is not null)
        {
            throw new InvalidOperationException("Second bitfield received, dropping connection");
        }

        PeerBitField = message.Bitfield;
        RegisterPieces(PeerBitField);
        CheckInterest();
    }

    private void ReceiveHave(Message.Have message)
    {
        //Lazy bitfield
        PeerBitField ??= new(MyBitField.Length);
        //If the have was already sent or we already know that he has that piece we omit this message
        if (PeerBitField.HasPiece(message.Index))
        {
            return;
        }

        RegisterPiece(message.Index);
        PeerBitField.SetPiece(message.Index);
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
        Message.RequestBlockMessage message,
        CancellationToken cancellationToken
    )
    {
        if (_amChoking.Value)
        {
            return;
        }

        var request = new RequestBlock(message.Index, message.Begin, message.Length)
        {
            TimeoutAt = null,
            RequestedFrom = [this],
        };
        await uploadScheduler.AddRequestAsync(request, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask ReceiveBlockAsync(
        Message.BlockMessage message,
        CancellationToken cancellationToken
    )
    {
        var block = new Block(message.Index, message.Begin, message.Payload, this);
        await requestScheduler.ReceiveBlockAsync(block, cancellationToken).ConfigureAwait(false);
        DownloadTracker.AddBytes(message.Payload.Length);
        _lastReceivedBlock = DateTimeOffset.UtcNow;
    }

    private void ReceiveCancel(Message.CancelMessage message)
    {
        var request = new RequestBlock(message.Index, message.Begin, message.Length)
        {
            RequestedFrom = [this],
        };
        uploadScheduler.CancelRequest(request);
    }

    private async Task SendHaveAsync(int pieceIndex, CancellationToken cancellationToken)
    {
        CheckInterest();
        var message = new Message.Have(pieceIndex);
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
        await WriteMessageAsync(new Message.BitfieldMessage(bitField), cancellationToken)
            .ConfigureAwait(false);
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
        if (messageStream.TrySend(message))
        {
            _lastSentMessageTime = DateTimeOffset.UtcNow;
            return true;
        }
        return false;
    }

    private async ValueTask WriteMessageAsync(Message message, CancellationToken cancellationToken)
    {
        await messageStream.SendAsync(message, cancellationToken).ConfigureAwait(false);
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
