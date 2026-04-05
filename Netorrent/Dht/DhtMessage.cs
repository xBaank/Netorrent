using System.Net;
using Netorrent.Dht.Krpc;
using Netorrent.Dht.Routing;

namespace Netorrent.Dht;

internal abstract record DhtMessage
{
    /// <summary>Bootstrap: connect to known DHT nodes and populate the routing table.</summary>
    internal sealed record BootstrapMessage : DhtMessage;

    /// <summary>Periodic peer discovery query for the tracked info hash.</summary>
    internal sealed record GetPeersMessage : DhtMessage;

    /// <summary>Periodic bucket refresh to maintain routing table health.</summary>
    internal sealed record RefreshBucketsMessage : DhtMessage;

    /// <summary>Rotate the announce token secret.</summary>
    internal sealed record RotateTokensMessage : DhtMessage;

    /// <summary>Ping a node to decide whether to evict it from a full bucket.</summary>
    internal sealed record PingNodeMessage(DhtNode Node) : DhtMessage;

    /// <summary>An incoming KRPC message forwarded from the handler receive loop.</summary>
    internal sealed record IncomingMessage(KrpcMessage Message, IPEndPoint Remote) : DhtMessage;
}
