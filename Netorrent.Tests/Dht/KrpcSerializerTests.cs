using System.Net;
using Netorrent.Dht.Krpc;
using Netorrent.Dht.Routing;
using Netorrent.TorrentFile.FileStructure;
using Shouldly;

namespace Netorrent.Tests.Dht;

public class KrpcSerializerTests
{
    private static NodeId MakeNodeId(byte fill = 0xAA)
    {
        var bytes = Enumerable.Repeat(fill, 20).Select(b => (byte)b).ToArray();
        return new(bytes);
    }

    private static InfoHash MakeInfoHash(byte fill = 0xBB) =>
        new(Enumerable.Repeat(fill, 20).Select(b => (byte)b).ToArray());

    private static readonly TransactionId TestTxId = new(0x1234);

    [Test]
    public async Task Should_Round_Trip_PingQuery(CancellationToken ct)
    {
        var original = new KrpcMessage.PingQuery(TestTxId, MakeNodeId());
        using var serialized = await KrpcSerializer.SerializeAsync(original, ct);
        var result = await KrpcSerializer.TryDeserializeAsync(serialized.Memory, ct);

        result.ShouldBeOfType<KrpcMessage.PingQuery>();
        var ping = (KrpcMessage.PingQuery)result!;
        ping.TransactionId.ShouldBe(TestTxId);
        ping.SenderId.ShouldBe(MakeNodeId());
    }

    [Test]
    public async Task Should_Round_Trip_PingResponse(CancellationToken ct)
    {
        var original = new KrpcMessage.PingResponse(TestTxId, MakeNodeId());
        using var serialized = await KrpcSerializer.SerializeAsync(original, ct);
        var result = await KrpcSerializer.TryDeserializeAsync(serialized.Memory, ct);

        result.ShouldBeOfType<KrpcMessage.PingResponse>();
        var resp = (KrpcMessage.PingResponse)result!;
        resp.TransactionId.ShouldBe(TestTxId);
        resp.ResponderId.ShouldBe(MakeNodeId());
    }

    [Test]
    public async Task Should_Round_Trip_FindNodeQuery(CancellationToken ct)
    {
        var original = new KrpcMessage.FindNodeQuery(TestTxId, MakeNodeId(0xAA), MakeNodeId(0xCC));
        using var serialized = await KrpcSerializer.SerializeAsync(original, ct);
        var result = await KrpcSerializer.TryDeserializeAsync(serialized.Memory, ct);

        result.ShouldBeOfType<KrpcMessage.FindNodeQuery>();
        var fn = (KrpcMessage.FindNodeQuery)result!;
        fn.SenderId.ShouldBe(MakeNodeId(0xAA));
        fn.Target.ShouldBe(MakeNodeId(0xCC));
    }

    [Test]
    public async Task Should_Round_Trip_FindNodeResponse_With_Nodes(CancellationToken ct)
    {
        var nodes = new List<DhtNode>
        {
            new(MakeNodeId(0x01), new IPEndPoint(IPAddress.Parse("192.168.1.1"), 6881)),
            new(MakeNodeId(0x02), new IPEndPoint(IPAddress.Parse("10.0.0.1"), 6882)),
        };
        var original = new KrpcMessage.FindNodeResponse(TestTxId, MakeNodeId(), nodes);
        using var serialized = await KrpcSerializer.SerializeAsync(original, ct);
        var result = await KrpcSerializer.TryDeserializeAsync(serialized.Memory, ct);

        result.ShouldBeOfType<KrpcMessage.FindNodeResponse>();
        var fn = (KrpcMessage.FindNodeResponse)result!;
        fn.Nodes.Count.ShouldBe(2);
        fn.Nodes[0].Id.ShouldBe(MakeNodeId(0x01));
        fn.Nodes[0].EndPoint.Port.ShouldBe(6881);
        fn.Nodes[1].EndPoint.Address.ToString().ShouldBe("10.0.0.1");
    }

    [Test]
    public async Task Should_Round_Trip_GetPeersQuery(CancellationToken ct)
    {
        var original = new KrpcMessage.GetPeersQuery(TestTxId, MakeNodeId(), MakeInfoHash());
        using var serialized = await KrpcSerializer.SerializeAsync(original, ct);
        var result = await KrpcSerializer.TryDeserializeAsync(serialized.Memory, ct);

        result.ShouldBeOfType<KrpcMessage.GetPeersQuery>();
        var gp = (KrpcMessage.GetPeersQuery)result!;
        gp.InfoHash.Data.ToArray().ShouldBe(MakeInfoHash().Data.ToArray());
    }

    [Test]
    public async Task Should_Round_Trip_GetPeersWithPeersResponse(CancellationToken ct)
    {
        var peers = new List<IPEndPoint>
        {
            new(IPAddress.Parse("1.2.3.4"), 6881),
            new(IPAddress.Parse("5.6.7.8"), 6882),
        };
        var token = new byte[] { 0xDE, 0xAD };
        var original = new KrpcMessage.GetPeersWithPeersResponse(
            TestTxId,
            MakeNodeId(),
            token,
            peers
        );
        using var serialized = await KrpcSerializer.SerializeAsync(original, ct);
        var result = await KrpcSerializer.TryDeserializeAsync(serialized.Memory, ct);

        result.ShouldBeOfType<KrpcMessage.GetPeersWithPeersResponse>();
        var gp = (KrpcMessage.GetPeersWithPeersResponse)result!;
        gp.Peers.Count.ShouldBe(2);
        gp.Peers[0].Address.ToString().ShouldBe("1.2.3.4");
        gp.Peers[1].Port.ShouldBe(6882);
        gp.Token.ShouldBe(token);
    }

    [Test]
    public async Task Should_Round_Trip_GetPeersWithNodesResponse(CancellationToken ct)
    {
        var nodes = new List<DhtNode>
        {
            new(MakeNodeId(0x03), new IPEndPoint(IPAddress.Parse("127.0.0.1"), 6883)),
        };
        var token = new byte[] { 0xBE, 0xEF };
        var original = new KrpcMessage.GetPeersWithNodesResponse(
            TestTxId,
            MakeNodeId(),
            token,
            nodes
        );
        using var serialized = await KrpcSerializer.SerializeAsync(original, ct);
        var result = await KrpcSerializer.TryDeserializeAsync(serialized.Memory, ct);

        result.ShouldBeOfType<KrpcMessage.GetPeersWithNodesResponse>();
        var gp = (KrpcMessage.GetPeersWithNodesResponse)result!;
        gp.Nodes.Count.ShouldBe(1);
        gp.Token.ShouldBe(token);
    }

    [Test]
    public async Task Should_Round_Trip_AnnouncePeerQuery(CancellationToken ct)
    {
        var original = new KrpcMessage.AnnouncePeerQuery(
            TestTxId,
            MakeNodeId(),
            MakeInfoHash(),
            Port: 6881,
            Token: [0xAB, 0xCD],
            ImpliedPort: false
        );
        using var serialized = await KrpcSerializer.SerializeAsync(original, ct);
        var result = await KrpcSerializer.TryDeserializeAsync(serialized.Memory, ct);

        result.ShouldBeOfType<KrpcMessage.AnnouncePeerQuery>();
        var ap = (KrpcMessage.AnnouncePeerQuery)result!;
        ap.Port.ShouldBe(6881);
        ap.ImpliedPort.ShouldBeFalse();
    }

    [Test]
    public async Task Should_Round_Trip_ErrorResponse(CancellationToken ct)
    {
        var original = new KrpcMessage.ErrorResponse(TestTxId, 203, "Method Unknown");
        using var serialized = await KrpcSerializer.SerializeAsync(original, ct);
        var result = await KrpcSerializer.TryDeserializeAsync(serialized.Memory, ct);

        result.ShouldBeOfType<KrpcMessage.ErrorResponse>();
        var err = (KrpcMessage.ErrorResponse)result!;
        err.ErrorCode.ShouldBe(203);
        err.ErrorMessage.ShouldBe("Method Unknown");
    }

    [Test]
    public async Task Should_Return_Null_For_Invalid_Data(CancellationToken ct)
    {
        var garbage = new byte[] { 0xFF, 0xFE, 0xFD };
        var result = await KrpcSerializer.TryDeserializeAsync(garbage, ct);
        result.ShouldBeNull();
    }

    [Test]
    public async Task Should_Return_Null_For_Empty_Data(CancellationToken ct)
    {
        var result = await KrpcSerializer.TryDeserializeAsync(Array.Empty<byte>(), ct);
        result.ShouldBeNull();
    }
}
