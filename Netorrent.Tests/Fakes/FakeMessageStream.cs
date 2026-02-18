using System.Threading.Channels;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Messages;

namespace Netorrent.Tests.Fakes;

internal class FakeMessageStream(
    PeerId otherPeerId,
    Channel<Message> incommingMessages,
    Channel<Message> outgoingMessages
) : IMessageStream
{
    public Handshake Handshake => new(0, string.Empty, new byte[20], otherPeerId.ToBytes());

    public ValueTask<PeerId> PerformHandshakeAsync(
        ReadOnlyMemory<byte> infoHash,
        PeerId peerId,
        CancellationToken cancellationToken = default
    ) => ValueTask.FromResult(otherPeerId);

    public ValueTask<PeerId> ReceiveHandshakeAsync(
        ReadOnlyMemory<byte> infoHash,
        PeerId peerId,
        CancellationToken cancellationToken = default
    ) => ValueTask.FromResult(otherPeerId);

    public async Task StartAsync(MessageHandler messageHandler, CancellationToken cancellationToken)
    {
        await foreach (
            var item in incommingMessages
                .Reader.ReadAllAsync(cancellationToken)
                .ConfigureAwait(false)
        )
        {
            using var message = item;
            await messageHandler(item, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask DrainChannelsAsync()
    {
        await foreach (var item in incommingMessages.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            item.Dispose();
        }
        await foreach (var item in outgoingMessages.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            item.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        incommingMessages.Writer.TryComplete();
        outgoingMessages.Writer.TryComplete();
        await DrainChannelsAsync();
        await incommingMessages.Reader.Completion;
        await outgoingMessages.Reader.Completion;
    }

    public ValueTask SendAsync(Message message, CancellationToken cancellationToken) =>
        outgoingMessages.Writer.WriteOrDisposeAsync(message, cancellationToken);

    public bool TrySend(Message message) => outgoingMessages.Writer.TryWriteOrDispose(message);
}
