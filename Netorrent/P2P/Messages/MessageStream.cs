using System.Buffers;
using System.Buffers.Binary;
using System.Threading.Channels;
using Netorrent.Extensions;
using Netorrent.Other;

namespace Netorrent.P2P.Messages;

internal class MessageStream(Stream stream, TimeSpan timeout) : IDisposable
{
    private CancellationTokenSource? _cancellationTokenSource;
    private readonly Channel<Message> _incomingMessages = Channel.CreateBounded<Message>(
        new BoundedChannelOptions(256) { SingleWriter = true, SingleReader = true }
    );
    private readonly Channel<Message> _outgoingMessages = Channel.CreateBounded<Message>(
        new BoundedChannelOptions(256) { SingleWriter = false, SingleReader = true }
    );

    public ChannelReader<Message> IncomingMessages => _incomingMessages.Reader;
    public ChannelWriter<Message> OutgoingMessages => _outgoingMessages.Writer;

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken
        );
        var readTask = ReadLoopAsync(_cancellationTokenSource.Token);
        var writeTask = WriteLoopAsync(_cancellationTokenSource.Token);
        var finishedTask = await Task.WhenAny(readTask, writeTask);
        if (finishedTask.IsFaulted)
        {
            _cancellationTokenSource.Cancel();
            _incomingMessages.Writer.TryComplete(finishedTask.Exception);
            _outgoingMessages.Writer.TryComplete(finishedTask.Exception);
        }
        await finishedTask;
    }

    public async ValueTask<PeerId> PerformHandshakeAsync(
        ReadOnlyMemory<byte> infoHash,
        PeerId peerId,
        CancellationToken cancellationToken = default
    )
    {
        using var timeoutCts = new CancellationTokenSource(timeout);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            timeoutCts.Token
        );
        using var bytesRented = await SendHandHandshake(infoHash, peerId, linkedCts.Token);
        var (pool, receivedHandshake) = await ReceiveHandshakeAsync(linkedCts.Token);

        using (pool)
        {
            return ValidateHandshake(infoHash, receivedHandshake);
        }
    }

    public async ValueTask<PeerId> ReceiveHandshakeAsync(
        ReadOnlyMemory<byte> infoHash,
        PeerId peerId,
        CancellationToken cancellationToken = default
    )
    {
        using var timeoutCts = new CancellationTokenSource(timeout);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            timeoutCts.Token
        );

        var (pool, receivedHandshake) = await ReceiveHandshakeAsync(linkedCts.Token);

        using (pool)
        {
            using var bytesRented = await SendHandHandshake(infoHash, peerId, linkedCts.Token);
            return ValidateHandshake(infoHash, receivedHandshake);
        }
    }

    private async Task ReadLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var message = await ReceiveMessageAsync(cancellationToken);
            await _incomingMessages.Writer.WriteAsync(message, cancellationToken);
        }
    }

    private async Task WriteLoopAsync(CancellationToken cancellationToken)
    {
        await foreach (var item in _outgoingMessages.Reader.ReadAllAsync(cancellationToken))
        {
            using var message = item;
            await SendMessageAsync(message, cancellationToken);
        }
    }

    private async ValueTask SendMessageAsync(
        Message message,
        CancellationToken cancellationToken = default
    )
    {
        using var cts = cancellationToken.WithTimeout(timeout);
        using var messageBytes = message.ToMemoryRented();
        await stream.WriteAsync(messageBytes.Memory, cts.Token);
        await stream.FlushAsync(cts.Token);
    }

    private async ValueTask<Message> ReceiveMessageAsync(
        CancellationToken cancellationToken = default
    )
    {
        using var cts = cancellationToken.WithTimeout(timeout);
        using var lengthPool = MemoryPool<byte>.Shared.Rent(4);
        var lengthBuffer = lengthPool.Memory[..4];
        await stream.ReadExactlyAsync(lengthBuffer, cts.Token);
        int messageLength = BinaryPrimitives.ReadInt32BigEndian(lengthBuffer.Span[..4]);
        if (messageLength == 0)
            return Message.CreateKeepAlive();
        var array = ArrayPool<byte>.Shared.Rent(lengthBuffer.Length + messageLength);
        var totalMessageBuffer = array.AsMemory()[..(lengthBuffer.Length + messageLength)];
        var messageBuffer = array.AsMemory().Slice(lengthBuffer.Length, messageLength);
        lengthBuffer.CopyTo(totalMessageBuffer);
        await stream.ReadExactlyAsync(messageBuffer, cts.Token);
        return Message.From(array, totalMessageBuffer.Length);
    }

    private async ValueTask<(
        IMemoryOwner<byte> pool,
        Handshake receivedHandshake
    )> ReceiveHandshakeAsync(CancellationToken cancellationToken)
    {
        using var cts = cancellationToken.WithTimeout(timeout);
        using var pool = MemoryPool<byte>.Shared.Rent(Handshake.TotalLength);
        var buffer = pool.Memory[..Handshake.TotalLength];
        await stream.ReadExactlyAsync(buffer, cts.Token);
        var receivedHandshake = Handshake.FromBytes(buffer.Span);
        return (pool, receivedHandshake);
    }

    private async ValueTask<RentedArray<byte>> SendHandHandshake(
        ReadOnlyMemory<byte> infoHash,
        PeerId peerId,
        CancellationToken cancellationToken
    )
    {
        using var cts = cancellationToken.WithTimeout(timeout);
        var handshake = Handshake.Create(infoHash.ToArray(), peerId.ToBytes());
        var bytesRented = handshake.ToBytes();
        await stream.WriteAsync(bytesRented.Memory, cts.Token);
        await stream.FlushAsync(cts.Token);
        return bytesRented;
    }

    private static PeerId ValidateHandshake(
        ReadOnlyMemory<byte> infoHash,
        Handshake receivedHandshake
    )
    {
        if (receivedHandshake.InfoHash.AsSpan().SequenceEqual(infoHash.Span) is false)
            throw new InvalidDataException("InfoHash mismatch in handshake.");

        return new PeerId(receivedHandshake.PeerId);
    }

    public void Dispose()
    {
        _cancellationTokenSource?.Cancel();
        stream.Dispose();
        _incomingMessages.Writer.TryComplete();
        _outgoingMessages.Writer.TryComplete();

        while (_incomingMessages.Reader.TryRead(out var message))
        {
            message.Dispose();
        }
        while (_outgoingMessages.Reader.TryRead(out var message))
        {
            message.Dispose();
        }

        _cancellationTokenSource?.Dispose();
    }
}
