using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Threading.Channels;
using Netorrent.Exceptions;
using Netorrent.Extensions;
using Netorrent.P2P.Messages;

namespace Netorrent.IO;

internal class MessageStream(Stream stream, Handshake handshake, TimeSpan timeout) : IMessageStream
{
    private readonly Channel<Message> _outgoingMessages = Channel.CreateBounded<Message>(
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
                    while (TryParseMessage(ref buffer, out var item))
                    {
                        using var message = item;
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

    public async ValueTask SendAsync(Message message, CancellationToken cancellationToken) =>
        await _outgoingMessages
            .Writer.WriteOrDisposeAsync(message, cancellationToken)
            .ConfigureAwait(false);

    public bool TrySend(Message message) => _outgoingMessages.Writer.TryWriteOrDispose(message);

    private bool TryParseMessage(ref ReadOnlySequence<byte> buffer, out Message message)
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
            message = Message.CreateKeepAlive();
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

        int payloadLength = length - 1;

        var array = ArrayPool<byte>.Shared.Rent(payloadLength);
        buffer.Slice(5, payloadLength).CopyTo(array);

        message = Message.From(array, payloadLength, messageId);

        // Consume the full message
        buffer = buffer.Slice(4 + length);
        return true;
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

    private async ValueTask DrainChannelsAsync()
    {
        await foreach (var item in _outgoingMessages.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            item.Dispose();
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
