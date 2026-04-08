using System.Net;
using Netorrent.Dht.Routing;
using Shouldly;

namespace Netorrent.Tests.Dht;

public class RoutingTableTests
{
    private static DhtNode MakeNode(byte leadingByte) =>
        new(new(MakeIdBytes(leadingByte)), new IPEndPoint(IPAddress.Loopback, 6881));

    private static byte[] MakeIdBytes(byte leadingByte)
    {
        var bytes = new byte[20];
        bytes[0] = leadingByte;
        return bytes;
    }

    [Test]
    public void Should_Insert_Node_Into_Routing_Table()
    {
        var selfId = new NodeId(new byte[20]);
        var table = new RoutingTable(selfId);

        var node = MakeNode(0b10000000); // XOR with selfId = 0b10000000 → bucket 0

        var evictCandidate = table.TryInsert(node);

        evictCandidate.ShouldBeNull();
        table.Count.ShouldBe(1);
    }

    [Test]
    public void Should_Not_Insert_Self()
    {
        var selfId = new NodeId(new byte[20]);
        var table = new RoutingTable(selfId);

        var selfNode = new DhtNode(selfId, new IPEndPoint(IPAddress.Loopback, 6881));
        var evictCandidate = table.TryInsert(selfNode);

        evictCandidate.ShouldBeNull();
        table.Count.ShouldBe(0);
    }

    [Test]
    public void Should_Return_Eviction_Candidate_When_Bucket_Full()
    {
        var selfId = new NodeId(new byte[20]);
        var table = new RoutingTable(selfId, k: 2); // k=2 to fill quickly

        // All these have MSB set, so they all go to bucket 0
        var nodes = Enumerable
            .Range(0, 3)
            .Select(i =>
            {
                var bytes = new byte[20];
                bytes[0] = 0b10000000;
                bytes[1] = (byte)i;
                return new DhtNode(new NodeId(bytes), new IPEndPoint(IPAddress.Loopback, 6881 + i));
            })
            .ToArray();

        table.TryInsert(nodes[0]).ShouldBeNull(); // bucket has room
        table.TryInsert(nodes[1]).ShouldBeNull(); // bucket now full
        var candidate = table.TryInsert(nodes[2]); // bucket full → returns LRS
        candidate.ShouldNotBeNull();
        candidate.Id.ShouldBe(nodes[0].Id); // LRS is the first-inserted node
    }

    [Test]
    public void Should_Return_K_Closest_Nodes()
    {
        var selfId = new NodeId(new byte[20]);
        var table = new RoutingTable(selfId);

        for (int i = 0; i < 20; i++)
        {
            var bytes = new byte[20];
            bytes[19] = (byte)i; // all differ only in last byte → bucket 159
            table.TryInsert(
                new DhtNode(new NodeId(bytes), new IPEndPoint(IPAddress.Loopback, 6881 + i))
            );
        }

        var target = new NodeId(new byte[20]);
        var closest = table.GetClosest(target, 5);
        closest.Count.ShouldBe(5);
    }

    [Test]
    public void Should_Refresh_Node_LastSeen()
    {
        var selfId = new NodeId(new byte[20]);
        var table = new RoutingTable(selfId);

        var bytes = new byte[20];
        bytes[0] = 0b10000000;
        var nodeId = new NodeId(bytes);
        table.TryInsert(new DhtNode(nodeId, new IPEndPoint(IPAddress.Loopback, 6881)));

        var before = DateTime.UtcNow;
        table.RefreshNode(nodeId);

        var bucket = table.GetBucketFor(nodeId);
        bucket.Nodes[0].LastSeen.ShouldBeGreaterThanOrEqualTo(before);
    }

    [Test]
    public void Should_Deduplicate_Same_Node_On_Insert()
    {
        var selfId = new NodeId(new byte[20]);
        var table = new RoutingTable(selfId);

        var bytes = new byte[20];
        bytes[0] = 0b10000000;
        var nodeId = new NodeId(bytes);
        var ep = new IPEndPoint(IPAddress.Loopback, 6881);

        table.TryInsert(new DhtNode(nodeId, ep));
        table.TryInsert(new DhtNode(nodeId, ep)); // re-insert same ID

        table.Count.ShouldBe(1); // still just one node
    }
}
