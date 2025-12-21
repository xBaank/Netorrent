using System.Net.Sockets;
using System.Threading.Channels;
using Netorrent.Extensions;
using Netorrent.P2P;
using Netorrent.P2P.Messages;

namespace Netorrent.IO;

internal class TcpMessageStream(TcpClient tcpClient) : IMessageStream, IHandshakeStream
{
    const int PEER_TIMEOUT_SECONDS = 120;

    private readonly MessageStream stream = new(
        tcpClient.GetStream(),
        PEER_TIMEOUT_SECONDS.Seconds
    );

    public ChannelReader<Message> IncomingMessages => stream.IncomingMessages;

    public ChannelWriter<Message> OutgoingMessages => stream.OutgoingMessages;

    public async ValueTask DisposeAsync()
    {
        await stream.DisposeAsync().ConfigureAwait(false);
        tcpClient.Dispose();
    }

    public ValueTask<Handshake> PerformHandshakeAsync(
        ReadOnlyMemory<byte> infoHash,
        PeerId peerId,
        CancellationToken cancellationToken
    ) => stream.PerformHandshakeAsync(infoHash, peerId, cancellationToken);

    public ValueTask<Handshake> ReceiveHandshakeAsync(
        ICollection<ReadOnlyMemory<byte>> infoHashes,
        PeerId peerId,
        CancellationToken cancellationToken
    ) => stream.ReceiveHandshakeAsync(infoHashes, peerId, cancellationToken);

    public Task StartAsync(CancellationToken cancellationToken) =>
        stream.StartAsync(cancellationToken);
}
