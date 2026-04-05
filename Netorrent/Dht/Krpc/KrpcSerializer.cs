using System.Buffers.Binary;
using System.Net;
using Netorrent.Bencoding;
using Netorrent.Bencoding.Structs;
using Netorrent.Dht.Routing;
using Netorrent.Other;
using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.Dht.Krpc;

internal static class KrpcSerializer
{
    private static readonly BString _keyT = "t";
    private static readonly BString _keyY = "y";
    private static readonly BString _keyQ = "q";
    private static readonly BString _keyA = "a";
    private static readonly BString _keyR = "r";
    private static readonly BString _keyE = "e";
    private static readonly BString _keyId = "id";
    private static readonly BString _keyNodes = "nodes";
    private static readonly BString _keyValues = "values";
    private static readonly BString _keyToken = "token";
    private static readonly BString _keyTarget = "target";
    private static readonly BString _keyInfoHash = "info_hash";
    private static readonly BString _keyPort = "port";
    private static readonly BString _keyImpliedPort = "implied_port";
    private static readonly BString _yQuery = "q";
    private static readonly BString _yResponse = "r";
    private static readonly BString _yError = "e";
    private static readonly BString _qPing = "ping";
    private static readonly BString _qFindNode = "find_node";
    private static readonly BString _qGetPeers = "get_peers";
    private static readonly BString _qAnnouncePeer = "announce_peer";

    /// <summary>
    /// Deserializes a raw UDP datagram into a <see cref="KrpcMessage"/>.
    /// Returns <c>null</c> if the data is invalid or unrecognized.
    /// </summary>
    public static async ValueTask<KrpcMessage?> TryDeserializeAsync(
        ReadOnlyMemory<byte> data,
        CancellationToken ct = default
    )
    {
        try
        {
            // Reuse BDecoder by wrapping the buffer in a MemoryStream
            var stream = new MemoryStream(data.ToArray(), writable: false);
            var decoder = new BDecoder(stream);
            var node = await decoder.DecodeAsync(ct).ConfigureAwait(false);

            if (node is not BDictionary dict)
                return null;

            if (!dict.Elements.TryGetValue(_keyT, out var tNode) || tNode is not BString tStr)
                return null;
            if (!dict.Elements.TryGetValue(_keyY, out var yNode) || yNode is not BString yStr)
                return null;

            var txId = TransactionId.FromBytes(tStr.RawData);
            var messageType = (string)yStr;

            if (messageType == (string)_yQuery)
                return ParseQuery(dict, txId);

            if (messageType == (string)_yResponse)
                return ParseResponse(dict, txId);

            if (messageType == (string)_yError)
                return ParseError(dict, txId);

            return null;
        }
        catch
        {
            return null;
        }
    }

    private static KrpcMessage? ParseQuery(BDictionary dict, TransactionId txId)
    {
        if (!dict.Elements.TryGetValue(_keyQ, out var qNode) || qNode is not BString qStr)
            return null;
        if (!dict.Elements.TryGetValue(_keyA, out var aNode) || aNode is not BDictionary args)
            return null;

        var queryName = (string)qStr;

        if (!args.Elements.TryGetValue(_keyId, out var idNode) || idNode is not BString idStr)
            return null;
        if (idStr.RawData.Length != 20)
            return null;
        var senderId = NodeId.FromBytes(idStr.RawData);

        if (queryName == (string)_qPing)
            return new KrpcMessage.PingQuery(txId, senderId);

        if (queryName == (string)_qFindNode)
        {
            if (
                !args.Elements.TryGetValue(_keyTarget, out var targetNode)
                || targetNode is not BString targetStr
            )
                return null;
            if (targetStr.RawData.Length != 20)
                return null;
            var target = NodeId.FromBytes(targetStr.RawData);
            return new KrpcMessage.FindNodeQuery(txId, senderId, target);
        }

        if (queryName == (string)_qGetPeers)
        {
            if (
                !args.Elements.TryGetValue(_keyInfoHash, out var ihNode)
                || ihNode is not BString ihStr
            )
                return null;
            if (ihStr.RawData.Length != 20)
                return null;
            var infoHash = new InfoHash(ihStr.RawData);
            return new KrpcMessage.GetPeersQuery(txId, senderId, infoHash);
        }

        if (queryName == (string)_qAnnouncePeer)
        {
            if (
                !args.Elements.TryGetValue(_keyInfoHash, out var ihNode)
                || ihNode is not BString ihStr
            )
                return null;
            if (
                !args.Elements.TryGetValue(_keyToken, out var tokenNode)
                || tokenNode is not BString tokenStr
            )
                return null;
            if (ihStr.RawData.Length != 20)
                return null;

            int port = 0;
            bool impliedPort = false;

            if (args.Elements.TryGetValue(_keyPort, out var portNode) && portNode is BInt portInt)
                port = (int)portInt.Data;
            if (args.Elements.TryGetValue(_keyImpliedPort, out var ipNode) && ipNode is BInt ipInt)
                impliedPort = ipInt.Data != 0;

            var infoHash = new InfoHash(ihStr.RawData);
            return new KrpcMessage.AnnouncePeerQuery(
                txId,
                senderId,
                infoHash,
                port,
                tokenStr.RawData,
                impliedPort
            );
        }

        return null;
    }

    private static KrpcMessage? ParseResponse(BDictionary dict, TransactionId txId)
    {
        if (!dict.Elements.TryGetValue(_keyR, out var rNode) || rNode is not BDictionary resp)
            return null;

        if (!resp.Elements.TryGetValue(_keyId, out var idNode) || idNode is not BString idStr)
            return null;
        if (idStr.RawData.Length != 20)
            return null;
        var responderId = NodeId.FromBytes(idStr.RawData);

        // get_peers with peers ("values")
        if (
            resp.Elements.TryGetValue(_keyValues, out var valuesNode)
            && valuesNode is BList valuesList
        )
        {
            var token =
                resp.Elements.TryGetValue(_keyToken, out var tk) && tk is BString tkStr
                    ? tkStr.RawData
                    : [];
            var peers = new List<IPEndPoint>();
            foreach (var item in valuesList.Elements)
            {
                if (item is BString peerStr && peerStr.RawData.Length == 6)
                    peers.Add(ParseCompactPeer(peerStr.RawData));
            }
            return new KrpcMessage.GetPeersWithPeersResponse(txId, responderId, token, peers);
        }

        // find_node or get_peers with nodes ("nodes")
        if (
            resp.Elements.TryGetValue(_keyNodes, out var nodesNode) && nodesNode is BString nodesStr
        )
        {
            var nodes = ParseCompactNodes(nodesStr.RawData);

            // If there's a token, it's a get_peers-with-nodes response
            if (resp.Elements.TryGetValue(_keyToken, out var tk) && tk is BString tkStr)
                return new KrpcMessage.GetPeersWithNodesResponse(
                    txId,
                    responderId,
                    tkStr.RawData,
                    nodes
                );

            return new KrpcMessage.FindNodeResponse(txId, responderId, nodes);
        }

        // ping or announce_peer response (only "id" in response dict)
        return new KrpcMessage.PingResponse(txId, responderId);
    }

    private static KrpcMessage? ParseError(BDictionary dict, TransactionId txId)
    {
        if (!dict.Elements.TryGetValue(_keyE, out var eNode) || eNode is not BList eList)
            return null;
        if (eList.Elements.Count < 2)
            return null;

        int code = eList.Elements[0] is BInt codeInt ? (int)codeInt.Data : 0;
        string msg = eList.Elements[1] is BString msgStr ? (string)msgStr : string.Empty;
        return new KrpcMessage.ErrorResponse(txId, code, msg);
    }

    /// <summary>
    /// Serializes a <see cref="KrpcMessage"/> into a pooled byte array.
    /// The caller must dispose the returned <see cref="RentedArray{T}"/>.
    /// </summary>
    public static async ValueTask<RentedArray<byte>> SerializeAsync(
        KrpcMessage message,
        CancellationToken ct = default
    )
    {
        var dict = MessageToDict(message);
        var ms = new MemoryStream();
        await using var encoder = new BEncoder(ms);
        await encoder.EncodeAsync(dict, ct).ConfigureAwait(false);
        var bytes = ms.ToArray();
        var rented = new RentedArray<byte>(bytes.Length);
        bytes.CopyTo(rented.Memory.Span);
        return rented;
    }

    private static BDictionary MessageToDict(KrpcMessage message)
    {
        var txBytes = message.TransactionId.ToBytes();

        return message switch
        {
            KrpcMessage.PingQuery q => new BDictionary(
                new Dictionary<BString, IBencodingNode>
                {
                    [_keyT] = new BString(txBytes),
                    [_keyY] = _yQuery,
                    [_keyQ] = _qPing,
                    [_keyA] = new BDictionary(
                        new Dictionary<BString, IBencodingNode>
                        {
                            [_keyId] = new BString(q.SenderId.ToBytes()),
                        }
                    ),
                }
            ),
            KrpcMessage.FindNodeQuery q => new BDictionary(
                new Dictionary<BString, IBencodingNode>
                {
                    [_keyT] = new BString(txBytes),
                    [_keyY] = _yQuery,
                    [_keyQ] = _qFindNode,
                    [_keyA] = new BDictionary(
                        new Dictionary<BString, IBencodingNode>
                        {
                            [_keyId] = new BString(q.SenderId.ToBytes()),
                            [_keyTarget] = new BString(q.Target.ToBytes()),
                        }
                    ),
                }
            ),
            KrpcMessage.GetPeersQuery q => new BDictionary(
                new Dictionary<BString, IBencodingNode>
                {
                    [_keyT] = new BString(txBytes),
                    [_keyY] = _yQuery,
                    [_keyQ] = _qGetPeers,
                    [_keyA] = new BDictionary(
                        new Dictionary<BString, IBencodingNode>
                        {
                            [_keyId] = new BString(q.SenderId.ToBytes()),
                            [_keyInfoHash] = new BString(q.InfoHash.Data.ToArray()),
                        }
                    ),
                }
            ),
            KrpcMessage.AnnouncePeerQuery q => new BDictionary(
                new Dictionary<BString, IBencodingNode>
                {
                    [_keyT] = new BString(txBytes),
                    [_keyY] = _yQuery,
                    [_keyQ] = _qAnnouncePeer,
                    [_keyA] = new BDictionary(
                        new Dictionary<BString, IBencodingNode>
                        {
                            [_keyId] = new BString(q.SenderId.ToBytes()),
                            [_keyInfoHash] = new BString(q.InfoHash.Data.ToArray()),
                            [_keyPort] = new BInt(q.Port),
                            [_keyToken] = new BString(q.Token),
                            [_keyImpliedPort] = new BInt(q.ImpliedPort ? 1 : 0),
                        }
                    ),
                }
            ),
            KrpcMessage.PingResponse r => new BDictionary(
                new Dictionary<BString, IBencodingNode>
                {
                    [_keyT] = new BString(txBytes),
                    [_keyY] = _yResponse,
                    [_keyR] = new BDictionary(
                        new Dictionary<BString, IBencodingNode>
                        {
                            [_keyId] = new BString(r.ResponderId.ToBytes()),
                        }
                    ),
                }
            ),
            KrpcMessage.FindNodeResponse r => new BDictionary(
                new Dictionary<BString, IBencodingNode>
                {
                    [_keyT] = new BString(txBytes),
                    [_keyY] = _yResponse,
                    [_keyR] = new BDictionary(
                        new Dictionary<BString, IBencodingNode>
                        {
                            [_keyId] = new BString(r.ResponderId.ToBytes()),
                            [_keyNodes] = new BString(SerializeCompactNodes(r.Nodes)),
                        }
                    ),
                }
            ),
            KrpcMessage.GetPeersWithPeersResponse r => new BDictionary(
                new Dictionary<BString, IBencodingNode>
                {
                    [_keyT] = new BString(txBytes),
                    [_keyY] = _yResponse,
                    [_keyR] = new BDictionary(
                        new Dictionary<BString, IBencodingNode>
                        {
                            [_keyId] = new BString(r.ResponderId.ToBytes()),
                            [_keyToken] = new BString(r.Token),
                            [_keyValues] = new BList(
                                r.Peers.Select(static p =>
                                        (IBencodingNode)new BString(SerializeCompactPeer(p))
                                    )
                                    .ToList()
                            ),
                        }
                    ),
                }
            ),
            KrpcMessage.GetPeersWithNodesResponse r => new BDictionary(
                new Dictionary<BString, IBencodingNode>
                {
                    [_keyT] = new BString(txBytes),
                    [_keyY] = _yResponse,
                    [_keyR] = new BDictionary(
                        new Dictionary<BString, IBencodingNode>
                        {
                            [_keyId] = new BString(r.ResponderId.ToBytes()),
                            [_keyToken] = new BString(r.Token),
                            [_keyNodes] = new BString(SerializeCompactNodes(r.Nodes)),
                        }
                    ),
                }
            ),
            KrpcMessage.AnnouncePeerResponse r => new BDictionary(
                new Dictionary<BString, IBencodingNode>
                {
                    [_keyT] = new BString(txBytes),
                    [_keyY] = _yResponse,
                    [_keyR] = new BDictionary(
                        new Dictionary<BString, IBencodingNode>
                        {
                            [_keyId] = new BString(r.ResponderId.ToBytes()),
                        }
                    ),
                }
            ),
            KrpcMessage.ErrorResponse r => new BDictionary(
                new Dictionary<BString, IBencodingNode>
                {
                    [_keyT] = new BString(txBytes),
                    [_keyY] = _yError,
                    [_keyE] = new BList(
                        new List<IBencodingNode>
                        {
                            new BInt(r.ErrorCode),
                            new BString(r.ErrorMessage),
                        }
                    ),
                }
            ),
            _ => throw new InvalidOperationException(
                $"Unknown message type: {message.GetType().Name}"
            ),
        };
    }

    // ── Compact node info: 26 bytes (20 id + 4 IPv4 + 2 port) ────────────────

    private static IReadOnlyList<DhtNode> ParseCompactNodes(byte[] data)
    {
        var nodes = new List<DhtNode>(data.Length / 26);
        for (int offset = 0; offset + 26 <= data.Length; offset += 26)
        {
            var idBytes = data[offset..(offset + 20)];
            var nodeId = NodeId.FromBytes(idBytes);
            var ipBytes = data[(offset + 20)..(offset + 24)];
            var ip = new System.Net.IPAddress(ipBytes);
            var port = BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(offset + 24, 2));
            nodes.Add(new DhtNode(nodeId, new System.Net.IPEndPoint(ip, port)));
        }
        return nodes;
    }

    private static byte[] SerializeCompactNodes(IReadOnlyList<DhtNode> nodes)
    {
        var result = new byte[nodes.Count * 26];
        for (int i = 0; i < nodes.Count; i++)
        {
            var offset = i * 26;
            var idBytes = nodes[i].Id.ToBytes();
            idBytes.CopyTo(result, offset);
            var ipBytes = nodes[i].EndPoint.Address.GetAddressBytes();
            if (ipBytes.Length == 4)
                ipBytes.CopyTo(result, offset + 20);
            BinaryPrimitives.WriteUInt16BigEndian(
                result.AsSpan(offset + 24),
                (ushort)nodes[i].EndPoint.Port
            );
        }
        return result;
    }

    // ── Compact peer info: 6 bytes (4 IPv4 + 2 port) ─────────────────────────

    private static System.Net.IPEndPoint ParseCompactPeer(byte[] data)
    {
        var ip = new System.Net.IPAddress(data[..4]);
        var port = BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(4, 2));
        return new System.Net.IPEndPoint(ip, port);
    }

    private static byte[] SerializeCompactPeer(System.Net.IPEndPoint endPoint)
    {
        var result = new byte[6];
        var ipBytes = endPoint.Address.GetAddressBytes();
        if (ipBytes.Length == 4)
            ipBytes.CopyTo(result, 0);
        BinaryPrimitives.WriteUInt16BigEndian(result.AsSpan(4), (ushort)endPoint.Port);
        return result;
    }
}
