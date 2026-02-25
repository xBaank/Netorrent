using System.Threading.Channels;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Messages;
using static Netorrent.P2P.Messages.IMessage;

namespace Netorrent.Tests.Fakes;

internal class FakeMessageStream(
    PeerId otherPeerId,
    Channel<IMessage> incommingMessages,
    Channel<IMessage> outgoingMessages
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
            await messageHandler(item, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask DrainChannelsAsync()
    {
        await foreach (var item in incommingMessages.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            if (item is BlockMessage blockMessage)
            {
                blockMessage.Dispose();
            }
        }
        await foreach (var item in outgoingMessages.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            if (item is BlockMessage blockMessage)
            {
                blockMessage.Dispose();
            }
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

    public ValueTask SendAsync(IMessage message, CancellationToken cancellationToken) =>
        outgoingMessages.Writer.WriteAsync(message, cancellationToken);

    public bool TrySend(IMessage message) => outgoingMessages.Writer.TryWrite(message);
}
