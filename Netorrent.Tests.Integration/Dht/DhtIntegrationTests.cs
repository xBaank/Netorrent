using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging.Abstractions;
using Netorrent.Dht;
using Netorrent.Dht.Krpc;
using Netorrent.Dht.Routing;
using Netorrent.Extensions;
using Netorrent.Tracker.Udp.Client;
using Shouldly;

namespace Netorrent.Tests.Integration.Dht;

/// <summary>
/// Integration tests for <see cref="DhtHandler"/> using two real UDP sockets on loopback.
/// No external network access required.
/// </summary>
[Timeout(15_000)]
public class DhtIntegrationTests
{
    private static (DhtHandler Handler, IPEndPoint EndPoint) CreateHandler()
    {
        var udpClient = UdpClient.GetFreeUdpClient(IPAddress.Loopback, 0);
        var ep = (IPEndPoint)udpClient.Client.LocalEndPoint!;
        var wrapper = new UdpClientWrapper(udpClient);
        var handler = new DhtHandler(wrapper, NullLogger.Instance, 2.Seconds, 500.Milliseconds, 2);
        return (handler, ep);
    }

    [Test]
    public async Task Two_Handlers_Can_Exchange_Ping_And_Pong(CancellationToken ct)
    {
        var (senderHandler, _) = CreateHandler();
        var (receiverHandler, receiverEp) = CreateHandler();

        var receiverNodeId = NodeId.Generate();
        var senderNodeId = NodeId.Generate();

        // The receiver auto-responds to ping via a DhtClient
        var channel = Channel.CreateUnbounded<IPEndPoint>();
        var receiverClient = new DhtClient(
            receiverNodeId,
            new Netorrent.TorrentFile.FileStructure.InfoHash(new byte[20]),
            receiverHandler,
            channel.Writer,
            new DhtClientOptions(Enabled: true, Port: 0, BootstrapNodes: []),
            6881,
            NullLogger.Instance
        );

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var receiverTask = receiverClient.StartAsync(cts.Token);

        // Let the receiver start up
        await Task.Delay(100, ct);

        // Send a ping from sender → receiver and expect a pong back
        var pingTxId = new TransactionId(0x0042);
        var ping = new KrpcMessage.PingQuery(pingTxId, senderNodeId);
        var response = await senderHandler.SendAndReceiveAsync(ping, receiverEp, ct);

        cts.Cancel();
        try { await receiverTask.ConfigureAwait(false); } catch { }
        await receiverClient.DisposeAsync();
        await senderHandler.DisposeAsync();

        response.ShouldBeOfType<KrpcMessage.PingResponse>();
        var pong = (KrpcMessage.PingResponse)response;
        pong.TransactionId.ShouldBe(pingTxId);
        pong.ResponderId.ShouldBe(receiverNodeId);
    }

    [Test]
    public async Task Two_Handlers_Can_Exchange_FindNode(CancellationToken ct)
    {
        var (senderHandler, _) = CreateHandler();
        var (receiverHandler, receiverEp) = CreateHandler();

        var receiverNodeId = NodeId.Generate();
        var senderNodeId = NodeId.Generate();
        var targetId = NodeId.Generate();

        var channel = Channel.CreateUnbounded<IPEndPoint>();
        var receiverClient = new DhtClient(
            receiverNodeId,
            new Netorrent.TorrentFile.FileStructure.InfoHash(new byte[20]),
            receiverHandler,
            channel.Writer,
            new DhtClientOptions(Enabled: true, Port: 0, BootstrapNodes: []),
            6881,
            NullLogger.Instance
        );

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var receiverTask = receiverClient.StartAsync(cts.Token);
        await Task.Delay(100, ct);

        var query = new KrpcMessage.FindNodeQuery(new TransactionId(0x0099), senderNodeId, targetId);
        var response = await senderHandler.SendAndReceiveAsync(query, receiverEp, ct);

        cts.Cancel();
        try { await receiverTask.ConfigureAwait(false); } catch { }
        await receiverClient.DisposeAsync();
        await senderHandler.DisposeAsync();

        response.ShouldBeOfType<KrpcMessage.FindNodeResponse>();
        var fn = (KrpcMessage.FindNodeResponse)response;
        fn.ResponderId.ShouldBe(receiverNodeId);
    }
}
