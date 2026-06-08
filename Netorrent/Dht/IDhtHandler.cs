using System.Net;
using System.Threading.Channels;
using Netorrent.Dht.Krpc;

namespace Netorrent.Dht;

internal interface IDhtHandler : IAsyncDisposable
{
    /// <summary>
    /// Unsolicited inbound traffic — incoming queries from remote nodes, plus any message
    /// that did not match a pending <see cref="SendAndReceiveAsync"/> call. Responses to our
    /// own queries are returned directly by <see cref="SendAndReceiveAsync"/> and are NOT
    /// published here.
    /// </summary>
    ChannelReader<(KrpcMessage Message, IPEndPoint Remote)> IncomingMessages { get; }

    /// <summary>
    /// Sends a message without waiting for a response.
    /// </summary>
    ValueTask SendAsync(
        KrpcMessage message,
        IPEndPoint remote,
        CancellationToken cancellationToken
    );

    /// <summary>
    /// Sends a query and waits for the matching response, retrying on timeout.
    /// </summary>
    ValueTask<KrpcMessage> SendAndReceiveAsync(
        KrpcMessage query,
        IPEndPoint remote,
        CancellationToken cancellationToken
    );
}
