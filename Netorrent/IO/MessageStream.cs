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
    private readonly Channel<Message> _incomingMessages = Channel.CreateBounded<Message>(
        new BoundedChannelOptions(128) { SingleWriter = true, SingleReader = true }
    );
    private readonly Channel<Message> _outgoingMessages = Channel.CreateBounded<Message>(
        new BoundedChannelOptions(128) { SingleWriter = false, SingleReader = true }
    );

    public Handshake Handshake => handshake;

    public ChannelReader<Message> IncomingMessages => _incomingMessages.Reader;
    public ChannelWriter<Message> OutgoingMessages => _outgoingMessages.Writer;

    private readonly byte[] _lengthBuffer = new byte[4];
    private readonly byte[] _idBuffer = new byte[1];
    private CancellationTokenSource? _receiveCts;
    private CancellationTokenSource? _sendCts;
    private CancellationTokenSource? _cancellationTokenSource;
    private Task? _runTask;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (_runTask is not null)
        {
            return _runTask;
        }

        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken
        );
        _runTask = _cancellationTokenSource.CancelOnFirstCompletionAndAwaitAllAsync([
            ReadLoopAsync(_cancellationTokenSource.Token),
            WriteLoopAsync(_cancellationTokenSource.Token),
        ]);
        return _runTask;
    }

    private async Task ReadLoopAsync(CancellationToken cancellationToken)
    {
        var reader = PipeReader.Create(
            stream,
            new StreamPipeReaderOptions(bufferSize: 32 * 1024, leaveOpen: true)
        );

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
                ReadResult result = await reader.ReadAsync(token);
                ReadOnlySequence<byte> buffer = result.Buffer;

                try
                {
                    var messageReceived = false;
                    // Process all messages from the buffer, modifying the input buffer on each
                    // iteration.
                    while (TryParseMessage(ref buffer, out Message message))
                    {
                        await _incomingMessages
                            .Writer.WriteOrDisposeAsync(message, cancellationToken)
                            .ConfigureAwait(false);
                        messageReceived = true;
                    }

                    if (messageReceived)
                    {
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

                    // There's no more data to be processed.
                    if (result.IsCompleted)
                    {
                        if (buffer.Length > 0)
                        {
                            // The message is incomplete and there's no more data to process.
                            throw new EndOfStreamException("Incomplete message.");
                        }
                        break;
                    }
                }
                finally
                {
                    // Since all messages in the buffer are being processed, you can use the
                    // remaining buffer's Start and End position to determine consumed and examined.
                    reader.AdvanceTo(buffer.Start, buffer.End);
                }
            }
        }
        finally
        {
            await reader.CompleteAsync();
        }
    }

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
        _cancellationTokenSource?.Cancel();
        await stream.DisposeAsync().ConfigureAwait(false);
        _incomingMessages.Writer.TryComplete();
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
        await _incomingMessages.Reader.Completion;
        await _outgoingMessages.Reader.Completion;
        _receiveCts?.Dispose();
        _sendCts?.Dispose();
        _cancellationTokenSource?.Dispose();
    }
}
