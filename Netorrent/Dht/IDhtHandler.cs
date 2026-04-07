using System.Net;
using Netorrent.Dht.Krpc;

namespace Netorrent.Dht;

internal interface IDhtHandler : IAsyncDisposable
{
    /// <summary>
    /// Raised for every received KRPC message — both responses (matched via transaction ID)
    /// and incoming queries from remote nodes.
    /// </summary>
    event Action<KrpcMessage, IPEndPoint>? MessageReceived;

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
