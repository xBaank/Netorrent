using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Netorrent.Extensions;
using Netorrent.P2P;
using Netorrent.P2P.Messages;

namespace Netorrent.IO;

internal class TcpMessageStream(TcpClient tcpClient, Handshake handshake) : IMessageStream
{
    const int PEER_TIMEOUT_SECONDS = 120;

    private readonly MessageStream stream = new(
        tcpClient.GetStream(),
        handshake,
        PEER_TIMEOUT_SECONDS.Seconds
    );

    public ChannelReader<Message> IncomingMessages => stream.IncomingMessages;

    public ChannelWriter<Message> OutgoingMessages => stream.OutgoingMessages;
    public PeerId PeerId => stream.PeerId;

    public async ValueTask DisposeAsync()
    {
        await stream.DisposeAsync().ConfigureAwait(false);
        tcpClient.Dispose();
    }

    public Task StartAsync(CancellationToken cancellationToken) =>
        stream.StartAsync(cancellationToken);
}
