using System.Buffers;
using System.Buffers.Binary;
using System.Threading.Channels;
using Netorrent.Extensions;
using Netorrent.P2P;
using Netorrent.P2P.Messages;
using ZLinq;

namespace Netorrent.IO;

internal class MessageStream(Stream stream, TimeSpan timeout) : IMessageStream, IHandshakeStream
{
    private readonly Channel<Message> _incomingMessages = Channel.CreateBounded<Message>(
        new BoundedChannelOptions(256) { SingleWriter = true, SingleReader = true }
    );
    private readonly Channel<Message> _outgoingMessages = Channel.CreateBounded<Message>(
        new BoundedChannelOptions(256) { SingleWriter = false, SingleReader = true }
    );

    public ChannelReader<Message> IncomingMessages => _incomingMessages.Reader;
    public ChannelWriter<Message> OutgoingMessages => _outgoingMessages.Writer;

    private readonly byte[] _lengthBuffer = new byte[4];
    private readonly byte[] _idBuffer = new byte[1];
    private CancellationTokenSource? _receiveCts;
    private CancellationTokenSource? _sendCts;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        await cts.CancelOnFirstCompletionAndAwaitAllAsync([
            ReadLoopAsync(cts.Token),
            WriteLoopAsync(cts.Token),
        ]);
    }

    public async ValueTask<Handshake> PerformHandshakeAsync(
        ReadOnlyMemory<byte> infoHash,
        PeerId peerId,
        CancellationToken cancellationToken
    )
    {
        using var timeoutCts = cancellationToken.WithTimeout(10.Seconds);
        await SendHandHandshake(infoHash, peerId, timeoutCts.Token).ConfigureAwait(false);
        var receivedHandshake = await ReceiveHandshakeAsync(timeoutCts.Token).ConfigureAwait(false);

        return receivedHandshake.InfoHash.SequenceEqual(infoHash.Span)
            ? receivedHandshake
            : throw new InvalidOperationException("InfoHash do not match");
    }

    public async ValueTask<Handshake> ReceiveHandshakeAsync(
        ICollection<ReadOnlyMemory<byte>> infoHashes,
        PeerId peerId,
        CancellationToken cancellationToken
    )
    {
        if (infoHashes.Count == 0)
        {
            throw new ArgumentException(
                "InfoHashes collection cannot be empty.",
                nameof(infoHashes)
            );
        }

        using var timeoutCts = cancellationToken.WithTimeout(10.Seconds);

        var receivedHandshake = await ReceiveHandshakeAsync(timeoutCts.Token).ConfigureAwait(false);

        ReadOnlyMemory<byte>? selectedInfoHash = null;
        foreach (var infoHash in infoHashes)
        {
            if (receivedHandshake.InfoHash.SequenceEqual(infoHash.Span))
            {
                selectedInfoHash = infoHash;
                break;
            }
        }

        if (selectedInfoHash is null)
        {
            throw new InvalidOperationException(
                "Received handshake contains an unknown info hash."
            );
        }

        await SendHandHandshake(selectedInfoHash.Value, peerId, timeoutCts.Token)
            .ConfigureAwait(false);

        return receivedHandshake;
    }

    private async Task ReadLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var message = await ReceiveMessageAsync(cancellationToken).ConfigureAwait(false);
            await _incomingMessages
                .Writer.WriteOrDisposeAsync(message, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private async Task WriteLoopAsync(CancellationToken cancellationToken)
    {
        await foreach (
            var item in _outgoingMessages
                .Reader.ReadAllAsync(cancellationToken)
                .ConfigureAwait(false)
        )
        {
            using var message = item;
            await SendMessageAsync(message, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask SendMessageAsync(Message message, CancellationToken cancellationToken)
    {
        if (_sendCts is null || !_sendCts.TryReset())
        {
            _sendCts?.Dispose();
            _sendCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        }
        _sendCts.CancelAfter(timeout);
        var token = _sendCts?.Token ?? cancellationToken;

        using var messageBytes = message.ToRentedArray();
        await stream.WriteAsync(messageBytes.Memory, token).ConfigureAwait(false);
        await stream.FlushAsync(token).ConfigureAwait(false);
    }

    private async ValueTask<Message> ReceiveMessageAsync(CancellationToken cancellationToken)
    {
        if (_receiveCts is null || !_receiveCts.TryReset())
        {
            _receiveCts?.Dispose();
            _receiveCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        }
        _receiveCts?.CancelAfter(timeout);
        var token = _receiveCts?.Token ?? cancellationToken;

        await stream.ReadExactlyAsync(_lengthBuffer, token).ConfigureAwait(false);
        int messageLength = BinaryPrimitives.ReadInt32BigEndian(_lengthBuffer);
        var payloadLength = messageLength - 1;

        if (messageLength == 0)
            return Message.CreateKeepAlive();

        var array = ArrayPool<byte>.Shared.Rent(messageLength);
        try
        {
            await stream.ReadExactlyAsync(_idBuffer, token).ConfigureAwait(false);
            await stream.ReadExactlyAsync(array, 0, payloadLength, token).ConfigureAwait(false);
            return Message.From(array, payloadLength, _idBuffer[0]);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(array);
            throw;
        }
    }

    private async ValueTask<Handshake> ReceiveHandshakeAsync(CancellationToken cancellationToken)
    {
        using var cts = cancellationToken.WithTimeout(timeout);
        using var pool = MemoryPool<byte>.Shared.Rent(Handshake.TotalLength);
        var buffer = pool.Memory[..Handshake.TotalLength];
        await stream.ReadExactlyAsync(buffer, cts.Token).ConfigureAwait(false);
        var receivedHandshake = Handshake.FromBytes(buffer.Span);
        return receivedHandshake;
    }

    private async ValueTask SendHandHandshake(
        ReadOnlyMemory<byte> infoHash,
        PeerId peerId,
        CancellationToken cancellationToken
    )
    {
        using var cts = cancellationToken.WithTimeout(timeout);
        var handshake = Handshake.Create(infoHash.ToArray(), peerId.ToBytes());
        using var bytesRented = handshake.ToBytes();
        await stream.WriteAsync(bytesRented.Memory, cts.Token).ConfigureAwait(false);
        await stream.FlushAsync(cts.Token).ConfigureAwait(false);
    }

    private static Handshake? ValidateHandshake(
        ReadOnlyMemory<byte> infoHash,
        Handshake receivedHandshake
    ) =>
        !receivedHandshake.InfoHash.SequenceEqual(infoHash.Span)
            ? throw new ArgumentException("")
            : receivedHandshake;

    private async ValueTask DrainChannelsAsync()
    {
        await foreach (var item in _incomingMessages.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            item.Dispose();
        }
        await foreach (var item in _outgoingMessages.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            item.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        stream.Dispose();
        _incomingMessages.Writer.TryComplete();
        _outgoingMessages.Writer.TryComplete();
        await DrainChannelsAsync().ConfigureAwait(false);
        await _incomingMessages.Reader.Completion;
        await _outgoingMessages.Reader.Completion;
        _receiveCts?.Dispose();
        _sendCts?.Dispose();
    }
}
