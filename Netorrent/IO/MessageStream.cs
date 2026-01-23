using System.Buffers;
using System.Buffers.Binary;
using System.Threading.Channels;
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
        {
            return Message.CreateKeepAlive();
        }

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
        stream.Dispose();
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
