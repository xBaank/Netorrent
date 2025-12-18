using System.Net;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Netorrent.P2P;
using Netorrent.P2P.Messages;
using Netorrent.Tests.Fakes;
using Shouldly;

namespace Netorrent.Tests.P2P;

internal class P2PClientTests
{
    [Test]
    [Timeout(20_000)]
    public async Task Should_connect_to_peers(CancellationToken cancellationToken)
    {
        var infoHash = new byte[20];
        var channel = Channel.CreateUnbounded<IPEndPoint>();
        var unusedChannel = Channel.CreateUnbounded<IPEndPoint>();
        var logger = NullLogger.Instance;
        var p2pClient = CreateP2PClient(infoHash, channel, logger);
        var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        P2PClient[] p2pClients =
        [
            CreateP2PClient(infoHash, unusedChannel, logger),
            CreateP2PClient(infoHash, unusedChannel, logger),
            CreateP2PClient(infoHash, unusedChannel, logger),
            CreateP2PClient(infoHash, unusedChannel, logger),
        ];
        var p2pClientsWithEndpoint = p2pClients
            .Select(i => (i, new IPEndPoint(IPAddress.IPv6Loopback, i.EndPoint.Port)))
            .OrderBy(i => i.Item2.Port)
            .ToList();
        var expectedEndpoints = p2pClientsWithEndpoint.Select(i => i.Item2).ToList();
        try
        {
            foreach (var (item, endpoint) in p2pClientsWithEndpoint)
            {
                _ = item.StartAsync(cts.Token);
                await channel.Writer.WriteAsync(endpoint, cts.Token);
            }

            var p2pTask = p2pClient.StartAsync(cts.Token);
            var peerConnections = await p2pClient.PeerConnected.Take(4).ToList().ToTask(cts.Token);
            cts.Cancel();

            peerConnections
                .Select(i => i.EndPoint)
                .OrderBy(i => i.Port)
                .ToList()
                .ShouldBeEquivalentTo(expectedEndpoints);

            await p2pTask.ShouldThrowAsync<OperationCanceledException>();
        }
        finally
        {
            await p2pClient.DisposeAsync();
            foreach (var item in p2pClients)
            {
                await item.DisposeAsync();
            }
        }
    }

    private static P2PClient CreateP2PClient(
        ReadOnlyMemory<byte> infoHash,
        ChannelReader<IPEndPoint> channel,
        ILogger logger
    ) =>
        new(
            infoHash,
            new PeerId(),
            new FakeRequestScheduler(),
            new FakeUploadScheduler(),
            new FakePiecePicker(),
            new Bitfield(5),
            channel,
            logger,
            null
        );
}
