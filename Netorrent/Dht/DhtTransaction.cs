using System.Net;
using Netorrent.Dht.Krpc;

namespace Netorrent.Dht;

internal sealed class DhtTransaction(
    KrpcMessage message,
    IPEndPoint remote,
    TaskCompletionSource<KrpcMessage> response
)
{
    public KrpcMessage Message { get; } = message;
    public IPEndPoint Remote { get; } = remote;
    public TaskCompletionSource<KrpcMessage> Response { get; } = response;
    public int RetryCount { get; set; }
    public DateTime NextRetryTime { get; set; }
}
