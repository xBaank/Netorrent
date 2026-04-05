using System.Net;
using Netorrent.Dht.Routing;
using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.Dht.Krpc;

internal readonly record struct TransactionId(ushort Value)
{
    public static TransactionId Generate() => new((ushort)Random.Shared.Next(0, 65536));

    public byte[] ToBytes() => [(byte)(Value >> 8), (byte)(Value & 0xFF)];

    public static TransactionId FromBytes(ReadOnlySpan<byte> bytes) =>
        bytes.Length >= 2 ? new((ushort)((bytes[0] << 8) | bytes[1]))
        : bytes.Length == 1 ? new((ushort)(bytes[0] << 8))
        : new(0);
}

internal abstract record KrpcMessage(TransactionId TransactionId)
{
    // ── Queries ───────────────────────────────────────────────────────────────
    internal sealed record PingQuery(TransactionId TransactionId, NodeId SenderId)
        : KrpcMessage(TransactionId);

    internal sealed record FindNodeQuery(
        TransactionId TransactionId,
        NodeId SenderId,
        NodeId Target
    ) : KrpcMessage(TransactionId);

    internal sealed record GetPeersQuery(
        TransactionId TransactionId,
        NodeId SenderId,
        InfoHash InfoHash
    ) : KrpcMessage(TransactionId);

    internal sealed record AnnouncePeerQuery(
        TransactionId TransactionId,
        NodeId SenderId,
        InfoHash InfoHash,
        int Port,
        byte[] Token,
        bool ImpliedPort
    ) : KrpcMessage(TransactionId);

    // ── Responses ─────────────────────────────────────────────────────────────
    internal sealed record PingResponse(TransactionId TransactionId, NodeId ResponderId)
        : KrpcMessage(TransactionId);

    internal sealed record FindNodeResponse(
        TransactionId TransactionId,
        NodeId ResponderId,
        IReadOnlyList<DhtNode> Nodes
    ) : KrpcMessage(TransactionId);

    internal sealed record GetPeersWithPeersResponse(
        TransactionId TransactionId,
        NodeId ResponderId,
        byte[] Token,
        IReadOnlyList<IPEndPoint> Peers
    ) : KrpcMessage(TransactionId);

    internal sealed record GetPeersWithNodesResponse(
        TransactionId TransactionId,
        NodeId ResponderId,
        byte[] Token,
        IReadOnlyList<DhtNode> Nodes
    ) : KrpcMessage(TransactionId);

    internal sealed record AnnouncePeerResponse(TransactionId TransactionId, NodeId ResponderId)
        : KrpcMessage(TransactionId);

    internal sealed record ErrorResponse(
        TransactionId TransactionId,
        int ErrorCode,
        string ErrorMessage
    ) : KrpcMessage(TransactionId);
}
