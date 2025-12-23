using System.Threading.Channels;
using Netorrent.IO;
using Netorrent.P2P;
using Netorrent.P2P.Messages;

namespace Netorrent.Tests.Fakes;

internal class FakeMessageStream(
    PeerId otherPeerId,
    Channel<Message> incommingMessages,
    Channel<Message> outgoingMessages
) : IMessageStream
{
    public ChannelReader<Message> IncomingMessages => incommingMessages;

    public ChannelWriter<Message> OutgoingMessages => outgoingMessages;

    public Handshake Handshake => new(0, string.Empty, [], otherPeerId.ToBytes());

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

    public Task StartAsync(CancellationToken cancellationToken) =>
        Task.Delay(-1, cancellationToken);

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
    }
}
