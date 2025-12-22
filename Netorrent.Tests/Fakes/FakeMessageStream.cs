using System.Threading.Channels;
using Netorrent.IO;
using Netorrent.P2P;
using Netorrent.P2P.Messages;

namespace Netorrent.Tests.Fakes;

internal class FakeMessageStream(
    PeerId otherPeerId,
    ChannelReader<Message> incommingMessages,
    ChannelWriter<Message> outgoingMessages
) : IMessageStream
{
    public ChannelReader<Message> IncomingMessages => incommingMessages;

    public ChannelWriter<Message> OutgoingMessages => outgoingMessages;

    public PeerId PeerId { get; } = new();

    public Handshake Handshake => new();

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

    public async ValueTask DisposeAsync()
    {
        outgoingMessages.TryComplete();
    }
}
