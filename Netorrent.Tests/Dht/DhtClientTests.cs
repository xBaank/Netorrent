using System.Net;
using System.Threading.Channels;
using Microsoft.Extensions.Logging.Abstractions;
using Netorrent.Dht;
using Netorrent.Dht.Krpc;
using Netorrent.Dht.Routing;
using Netorrent.Tests.Fakes;
using Netorrent.TorrentFile.FileStructure;
using Shouldly;

namespace Netorrent.Tests.Dht;

[Timeout(10_000)]
public class DhtClientTests
{
    private static NodeId MakeSelfId() => new(new byte[20]);

    private static NodeId MakeNodeId(byte b)
    {
        var bytes = new byte[20];
        bytes[0] = b;
        return new(bytes);
    }

    private static InfoHash MakeInfoHash() => new(Enumerable.Repeat((byte)0xBB, 20).ToArray());

    private static IPEndPoint MakeEndPoint(int port = 6881) => new(IPAddress.Loopback, port);

    /// <summary>
    /// Options with short intervals so tests don't wait 30s for get_peers.
    /// </summary>
    private static DhtClientOptions FastOptions() =>
        new DhtClientOptions(Enabled: true, Port: 0, BootstrapNodes: [])
        {
            // Fire first get_peers 500ms after start so tests can seed the routing table first
            GetPeersDelay = TimeSpan.FromMilliseconds(500),
            GetPeersInterval = TimeSpan.FromSeconds(10),
            RefreshInterval = TimeSpan.FromMinutes(60),
        };

    [Test]
    public async Task Should_Write_Peers_To_Channel_From_GetPeers_Response(CancellationToken ct)
    {
        var handler = new FakeDhtHandler();
        var channel = Channel.CreateUnbounded<IPEndPoint>();
        var selfId = MakeSelfId();
        var remoteNodeId = MakeNodeId(0x80); // bucket 0
        var infoHash = MakeInfoHash();

        // get_peers returns two peers when queried
        var discoveredPeers = new List<IPEndPoint>
        {
            new(IPAddress.Parse("1.2.3.4"), 6881),
            new(IPAddress.Parse("5.6.7.8"), 6882),
        };
        handler.SetupResponse<KrpcMessage.GetPeersQuery>(
            q => new KrpcMessage.GetPeersWithPeersResponse(
                q.TransactionId,
                remoteNodeId,
                [0xAB],
                discoveredPeers
            )
        );

        var client = new DhtClient(
            selfId,
            infoHash,
            handler,
            channel.Writer,
            FastOptions(),
            6881,
            NullLogger.Instance
        );

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var clientTask = client.StartAsync(cts.Token);

        // Let bootstrap complete (fast: no bootstrap nodes)
        await Task.Delay(100, ct);

        // Seed the routing table by simulating an incoming ping from a remote node.
        // The actor will process this PingQuery and call InsertNode → routing table populated.
        var pingQuery = new KrpcMessage.PingQuery(new TransactionId(0x0001), remoteNodeId);
        handler.SimulateIncoming(pingQuery, MakeEndPoint(7001));

        // Give the actor time to process the ping (insert node into routing table)
        await Task.Delay(100, ct);

        // GetPeers fires at 500ms from start → will now find the seeded node → discovers peers

        // Now wait for get_peers to fire (50ms delay) and write peers to channel
        var peers = await channel
            .Reader.ReadAllAsync(ct)
            .Take(2)
            .ToArrayAsync(cancellationToken: ct)
            .AsTask();

        cts.Cancel();
        try
        {
            await clientTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException) { }
        await client.DisposeAsync();

        peers.Length.ShouldBe(2);
        peers.ShouldContain(p => p.Address.ToString() == "1.2.3.4");
        peers.ShouldContain(p => p.Address.ToString() == "5.6.7.8");
    }

    [Test]
    public async Task Should_Respond_To_Ping_Query(CancellationToken ct)
    {
        var handler = new FakeDhtHandler();
        var channel = Channel.CreateUnbounded<IPEndPoint>();
        var selfId = MakeSelfId();
        var infoHash = MakeInfoHash();

        var client = new DhtClient(
            selfId,
            infoHash,
            handler,
            channel.Writer,
            FastOptions(),
            6881,
            NullLogger.Instance
        );

        var remote = MakeEndPoint(9000);
        var pingTxId = new TransactionId(0xABCD);
        var pingQuery = new KrpcMessage.PingQuery(pingTxId, MakeNodeId(0x42));

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var clientTask = client.StartAsync(cts.Token);

        // Give the actor a moment to start
        await Task.Delay(100, ct);

        // Simulate an incoming ping
        handler.SimulateIncoming(pingQuery, remote);

        // Wait briefly for the pong to be sent
        await Task.Delay(200, ct);

        cts.Cancel();
        try
        {
            await clientTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException) { }
        await client.DisposeAsync();

        // The handler should have sent a PingResponse
        var pong = handler.SentMessages.FirstOrDefault(m => m.Message is KrpcMessage.PingResponse);
        pong.Message.ShouldNotBeNull();
        var pongResp = (KrpcMessage.PingResponse)pong.Message;
        pongResp.TransactionId.ShouldBe(pingTxId);
    }

    [Test]
    public async Task Should_Query_Seeded_Nodes_For_GetPeers(CancellationToken ct)
    {
        var handler = new FakeDhtHandler();
        var channel = Channel.CreateUnbounded<IPEndPoint>();
        var selfId = MakeSelfId();
        var infoHash = MakeInfoHash();
        var remoteNodeId = MakeNodeId(0x80);

        handler.SetupResponse<KrpcMessage.GetPeersQuery>(
            _ => new KrpcMessage.GetPeersWithNodesResponse(
                TransactionId.Generate(),
                remoteNodeId,
                [],
                []
            )
        );

        var client = new DhtClient(
            selfId,
            infoHash,
            handler,
            channel.Writer,
            FastOptions(),
            6881,
            NullLogger.Instance
        );

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var clientTask = client.StartAsync(cts.Token);

        await Task.Delay(50, ct);

        // Seed routing table via incoming ping
        handler.SimulateIncoming(
            new KrpcMessage.PingQuery(new TransactionId(0x0001), remoteNodeId),
            MakeEndPoint(7001)
        );

        // Wait for get_peers to fire (delay is 500ms from start)
        await Task.Delay(700, ct);

        cts.Cancel();
        try
        {
            await clientTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException) { }
        await client.DisposeAsync();

        var getPeersMessages = handler
            .SentMessages.Where(m => m.Message is KrpcMessage.GetPeersQuery)
            .ToList();
        getPeersMessages.Count.ShouldBeGreaterThan(0);
    }
}
