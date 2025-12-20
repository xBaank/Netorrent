using System.Net;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Netorrent.P2P;
using Netorrent.P2P.Messages;
using Netorrent.Tests.Extensions;
using Netorrent.Tests.Fakes;
using R3;
using Shouldly;
using ZLinq;

namespace Netorrent.Tests.P2P;

[Timeout(10_000)]
internal class P2PClientTests
{
    [Test]
    [MatrixDataSource]
    public async Task Should_Connect_To_Peers(
        [MatrixRange<int>(1, 10)] int number,
        CancellationToken cancellationToken
    )
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var infoHash = new byte[20];
        var channel = Channel.CreateUnbounded<IPEndPoint>();
        var unusedChannel = Channel.CreateUnbounded<IPEndPoint>();
        var logger = NullLogger.Instance;
        await using var p2pClient = CreateP2PClient(infoHash, channel, logger);
        var p2pClients = CreateP2pClients(number, infoHash, unusedChannel, logger);

        var expectedEndpoints = p2pClients
            .Select(i => new IPEndPoint(IPAddress.IPv6Loopback, i.EndPoint.Port))
            .OrderBy(i => i.Port)
            .ToList();
        var peerEndpointsObservable = p2pClient.PeerConnected.Take(number).Select(i => i.EndPoint);

        await StartAsync(p2pClients, cts.Token);
        await WriteToChannelAsync(channel, p2pClients, cts.Token);
        var p2pTask = p2pClient.StartAsync(cts.Token);
        var peerEndpoints = (await peerEndpointsObservable.ToListAsync(cts.Token))
            .OrderBy(i => i.Port)
            .ToList();
        cts.Cancel();
        await DisposeP2pClients(p2pClients);

        peerEndpoints.ShouldBeEquivalentTo(expectedEndpoints);
        await p2pTask.ShouldThrowAsync<OperationCanceledException>();
    }

    private static async ValueTask DisposeP2pClients(IEnumerable<P2PClient> p2PClients)
    {
        foreach (var item in p2PClients)
        {
            await item.DisposeAsync();
        }
    }

    private static P2PClient[] CreateP2pClients(
        int number,
        ReadOnlyMemory<byte> infoHash,
        ChannelReader<IPEndPoint> channel,
        ILogger logger
    ) =>
        ValueEnumerable
            .Range(0, number)
            .Select(i => CreateP2PClient(infoHash, channel, logger))
            .ToArray();

    private static async Task StartAsync(
        IEnumerable<P2PClient> p2pClients,
        CancellationToken cancellationToken
    )
    {
        foreach (var item in p2pClients)
        {
            _ = item.StartAsync(cancellationToken);
        }
    }

    private static async Task WriteToChannelAsync(
        Channel<IPEndPoint> channel,
        IEnumerable<P2PClient> p2pClients,
        CancellationToken cancellationToken
    )
    {
        foreach (var item in p2pClients)
        {
            await channel.Writer.WriteAsync(
                new IPEndPoint(IPAddress.IPv6Loopback, item.EndPoint.Port),
                cancellationToken
            );
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
