using System.Net.Sockets;
using Netorrent.Extensions;
using Netorrent.P2P.Messages;

namespace Netorrent.IO;

internal class TcpMessageStream(TcpClient tcpClient, Handshake handshake, Bitfield bitfield)
    : IMessageStream
{
    const int PEER_TIMEOUT_SECONDS = 120;

    private readonly MessageStream stream = new(
        tcpClient.GetStream(),
        handshake,
        PEER_TIMEOUT_SECONDS.Seconds,
        bitfield
    );

    public Handshake Handshake => stream.Handshake;

    public async ValueTask DisposeAsync()
    {
        await stream.DisposeAsync().ConfigureAwait(false);
        tcpClient.Dispose();
    }

    public ValueTask SendAsync(Message message, CancellationToken cancellationToken) =>
        stream.SendAsync(message, cancellationToken);

    public Task StartAsync(MessageHandler messageHandler, CancellationToken cancellationToken) =>
        stream.StartAsync(messageHandler, cancellationToken);

    public bool TrySend(Message message) => stream.TrySend(message);
}
