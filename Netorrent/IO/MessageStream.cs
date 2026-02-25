using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Threading.Channels;
using Netorrent.Exceptions;
using Netorrent.Extensions;
using Netorrent.Other;
using Netorrent.P2P.Messages;
using static Netorrent.P2P.Messages.IMessage;

namespace Netorrent.IO;

internal class MessageStream(
    Stream stream,
    Handshake handshake,
    TimeSpan timeout,
    Bitfield myBitfield
) : IMessageStream
{
    private readonly Channel<IMessage> _outgoingMessages = Channel.CreateBounded<IMessage>(
        new BoundedChannelOptions(128) { SingleWriter = false, SingleReader = true }
    );

    private readonly PipeReader _reader = PipeReader.Create(
        stream,
        new StreamPipeReaderOptions(bufferSize: 32 * 1024, leaveOpen: true)
    );

    public Handshake Handshake => handshake;

    private readonly byte[] _lengthBuffer = new byte[4];
    private readonly byte[] _idBuffer = new byte[1];
    private CancellationTokenSource? _receiveCts;
    private CancellationTokenSource? _sendCts;
    private CancellationTokenSource? _cancellationTokenSource;
    private Task? _runTask;

    public Task StartAsync(MessageHandler messageHandler, CancellationToken cancellationToken)
    {
        if (_runTask is not null)
        {
            return _runTask;
        }

        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken
        );
        _runTask = _cancellationTokenSource.CancelOnFirstCompletionAndAwaitAllAsync([
            ReadLoopAsync(messageHandler, _cancellationTokenSource.Token),
            WriteLoopAsync(_cancellationTokenSource.Token),
        ]);
        return _runTask;
    }

    private async Task ReadLoopAsync(
        MessageHandler messageHandler,
        CancellationToken cancellationToken
    )
    {
        if (_receiveCts is null || !_receiveCts.TryReset())
        {
            _receiveCts?.Dispose();
            _receiveCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        }
        _receiveCts?.CancelAfter(timeout);
        var token = _receiveCts?.Token ?? cancellationToken;

        try
        {
            while (true)
            {
                ReadResult result = await _reader.ReadAsync(token).ConfigureAwait(false);
                ReadOnlySequence<byte> buffer = result.Buffer;

                try
                {
                    while (TryParseMessage(ref buffer, out var message))
                    {
                        await messageHandler(message, cancellationToken).ConfigureAwait(false);
                    }

                    if (result.IsCompleted)
                    {
                        if (buffer.Length > 0)
                        {
                            throw new EndOfStreamException("Incomplete message.");
                        }
                        break;
                    }
                }
                finally
                {
                    _reader.AdvanceTo(buffer.Start, buffer.End);
                }

                if (_receiveCts is null || !_receiveCts.TryReset())
                {
                    _receiveCts?.Dispose();
                    _receiveCts = CancellationTokenSource.CreateLinkedTokenSource(
                        cancellationToken
                    );
                }
                _receiveCts?.CancelAfter(timeout);
                token = _receiveCts?.Token ?? cancellationToken;
            }
        }
        finally
        {
            await _reader.CompleteAsync();
        }
    }

    public async ValueTask SendAsync(IMessage message, CancellationToken cancellationToken) =>
        await _outgoingMessages
            .Writer.WriteOrDisposeAsync(message, cancellationToken)
            .ConfigureAwait(false);

    public bool TrySend(IMessage message) => _outgoingMessages.Writer.TryWriteOrDispose(message);

    private bool TryParseMessage(
        ref ReadOnlySequence<byte> buffer,
        [NotNullWhen(true)] out IMessage? message
    )
    {
        message = default;

        // Need at least 4 bytes for length prefix
        if (buffer.Length < 4)
        {
            return false;
        }

        // Read length prefix (big endian)
        int length;
        if (buffer.First.Length >= 4)
        {
            length = BinaryPrimitives.ReadInt32BigEndian(buffer.First.Span.Slice(0, 4));
        }
        else
        {
            Span<byte> lengthSpan = stackalloc byte[4];
            buffer.Slice(0, 4).CopyTo(lengthSpan);
            length = BinaryPrimitives.ReadInt32BigEndian(lengthSpan);
        }

        if (length < 0 || length > 1024 * 1024)
        {
            throw new BitorrentProtocolViolationException("Invalid message length");
        }

        // Keep-alive message
        if (length == 0)
        {
            message = KeepAlive.Value;
            buffer = buffer.Slice(4); // consume the 4-byte length
            return true;
        }

        // Wait until full message is available
        if (buffer.Length < 4 + length)
        {
            return false;
        }

        // First byte is message ID
        byte messageId = buffer.Slice(4, 1).First.Span[0];

        if (messageId == IdChoke)
        {
            message = Choke.Value;
            buffer = buffer.Slice(4 + length);

            return true;
        }

        if (messageId == IdUnchoke)
        {
            message = Unchoke.Value;
            buffer = buffer.Slice(4 + length);

            return true;
        }
        if (messageId == IdInterested)
        {
            message = Interested.Value;
            buffer = buffer.Slice(4 + length);

            return true;
        }
        if (messageId == IdNotInterested)
        {
            message = NotInterested.Value;
            buffer = buffer.Slice(4 + length);
            return true;
        }

        if (messageId == IdHave)
        {
            int payloadLength = length - 1;

            if (payloadLength < 4)
            {
                return false;
            }

            Span<byte> span = stackalloc byte[payloadLength];
            buffer.Slice(5, payloadLength).CopyTo(span);

            var index = BinaryPrimitives.ReadInt32BigEndian(span);

            message = new Have(index);

            // Consume the full message
            buffer = buffer.Slice(4 + length);
            return true;
        }

        if (messageId == IdBitfield)
        {
            int payloadLength = length - 1;
            using var memoryOwner = MemoryPool<byte>.Shared.Rent(payloadLength);

            Span<byte> span = memoryOwner.Memory.Span[..payloadLength];
            buffer.Slice(5, payloadLength).CopyTo(span);

            message = new BitfieldMessage(new Bitfield(span, myBitfield.Length));

            // Consume the full message
            buffer = buffer.Slice(4 + length);
            return true;
        }

        if (messageId == IdRequest)
        {
            int payloadLength = length - 1;

            if (payloadLength < 12)
            {
                return false;
            }

            using var memoryOwner = MemoryPool<byte>.Shared.Rent(payloadLength);

            Span<byte> span = memoryOwner.Memory.Span[..payloadLength];
            buffer.Slice(5, payloadLength).CopyTo(span);

            var index = BinaryPrimitives.ReadInt32BigEndian(span[..4]);
            var begin = BinaryPrimitives.ReadInt32BigEndian(span[4..8]);
            var rlength = BinaryPrimitives.ReadInt32BigEndian(span[8..12]);

            message = new RequestBlockMessage(index, begin, rlength);

            // Consume the full message
            buffer = buffer.Slice(4 + length);
            return true;
        }

        if (messageId == IdCancel)
        {
            int payloadLength = length - 1;

            if (payloadLength < 12)
            {
                return false;
            }

            using var memoryOwner = MemoryPool<byte>.Shared.Rent(payloadLength);

            Span<byte> span = memoryOwner.Memory.Span[..payloadLength];
            buffer.Slice(5, payloadLength).CopyTo(span);

            var index = BinaryPrimitives.ReadInt32BigEndian(span[..4]);
            var begin = BinaryPrimitives.ReadInt32BigEndian(span[4..8]);
            var rlength = BinaryPrimitives.ReadInt32BigEndian(span[8..12]);

            message = new CancelMessage(index, begin, rlength);

            // Consume the full message
            buffer = buffer.Slice(4 + length);
            return true;
        }

        if (messageId == IdPiece)
        {
            int payloadLength = length - 1;

            if (payloadLength < 8)
            {
                return false;
            }

            using var memoryOwner = MemoryPool<byte>.Shared.Rent(8);

            Span<byte> span = memoryOwner.Memory.Span[..8];
            buffer.Slice(5, 8).CopyTo(span);

            int index = BinaryPrimitives.ReadInt32BigEndian(span[..4]);
            int begin = BinaryPrimitives.ReadInt32BigEndian(span[4..8]);
            payloadLength = payloadLength - 8;
            var rentedArray = new RentedArray<byte>(payloadLength);
            buffer.Slice(13, payloadLength).CopyTo(rentedArray.Memory.Span);

            message = new BlockMessage(index, begin, rentedArray);

            // Consume the full message
            buffer = buffer.Slice(4 + length);
            return true;
        }

        return false;
    }

    private async Task WriteLoopAsync(CancellationToken cancellationToken)
    {
        await foreach (
            var message in _outgoingMessages
                .Reader.ReadAllAsync(cancellationToken)
                .ConfigureAwait(false)
        )
        {
            await SendMessageAsync(message, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask SendMessageAsync(IMessage message, CancellationToken cancellationToken)
    {
        if (_sendCts is null || !_sendCts.TryReset())
        {
            _sendCts?.Dispose();
            _sendCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        }
        _sendCts.CancelAfter(timeout);
        var token = _sendCts?.Token ?? cancellationToken;

        using var rentedArray = message switch
        {
            Choke choke => SerializeChoke(choke),
            Unchoke unchoke => SerializeUnChoke(unchoke),
            Interested interested => SerializeInterested(interested),
            NotInterested notInterested => SerializeNotInterested(notInterested),
            KeepAlive keepAlive => SerializeKeepAlive(keepAlive),
            Have have => SerializeHave(have),
            CancelMessage cancelMessage => SerializeCancel(cancelMessage),
            BlockMessage blockMessage => SerializeBlock(blockMessage),
            RequestBlockMessage requestBlockMessage => SerializeRequest(requestBlockMessage),
            BitfieldMessage bitfieldMessage => SerializeBitfield(bitfieldMessage),
            Port port => SerializePort(port),
            _ => throw new InvalidOperationException(),
        };

        await stream.WriteAsync(rentedArray.Memory, token).ConfigureAwait(false);
        await stream.FlushAsync(token).ConfigureAwait(false);
    }

    private async ValueTask DrainChannelsAsync()
    {
        await foreach (var item in _outgoingMessages.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            if (item is BlockMessage blockMessage)
            {
                blockMessage.Dispose();
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cancellationTokenSource?.Cancel();
        await stream.DisposeAsync().ConfigureAwait(false);
        _outgoingMessages.Writer.TryComplete();
        try
        {
            if (_runTask is not null)
            {
                await _runTask.ConfigureAwait(false);
            }
        }
        catch { }
        await DrainChannelsAsync().ConfigureAwait(false);
        await _outgoingMessages.Reader.Completion;
        _receiveCts?.Dispose();
        _sendCts?.Dispose();
        _cancellationTokenSource?.Dispose();
    }
}
