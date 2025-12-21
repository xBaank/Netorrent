using System.Net;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Netorrent.Extensions;
using Netorrent.P2P;
using Netorrent.P2P.Messages;
using Netorrent.Tests.Extensions;
using Netorrent.Tests.Fakes;
using R3;
using Shouldly;
using ZLinq;

namespace Netorrent.Tests.P2P;

[Timeout(10_000)]
internal class PeersClientTests
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
        var unusedChannel = Channel.CreateUnbounded<IPEndPoint>();
        var logger = NullLogger.Instance;
        var peerId = new PeerId();
        await using var peersClient = CreatePeersClient(peerId, infoHash, unusedChannel, logger);
        await using var peerListener = CreatePeersListener(peerId, logger);
        var peersClients = CreatePeersClients(number, infoHash, logger);
        var p2pTask = peersClient.StartAsync(cts.Token);
        peerListener.Start();
        peerListener.AddPeersClient(infoHash, peersClient);

        var peerEndpointsObservable = peersClient
            .PeerConnected.Take(number)
            .Select(i => i.EndPoint);

        await StartAsync(peersClients.Select(i => i.peersClient), cts.Token);
        await WriteToChannels(peerListener, peersClients.Select(i => i.channel), cts.Token);

        var peerEndpointsCount = await peerEndpointsObservable.CountAsync(cts.Token);
        cts.Cancel();
        await DisposeP2pClients(peersClients.Select(i => i.peersClient));

        peerEndpointsCount.ShouldBe(number);
        await p2pTask.ShouldThrowAsync<OperationCanceledException>();
    }

    private static async Task WriteToChannels(
        PeersListener peerListener,
        IEnumerable<Channel<IPEndPoint>> channels,
        CancellationToken cancellationToken
    )
    {
        foreach (var channel in channels)
        {
            await channel.Writer.WriteAsync(
                new IPEndPoint(IPAddress.Loopback, peerListener.EndPoint.Port),
                cancellationToken
            );
        }
    }

    private static async ValueTask DisposeP2pClients(IEnumerable<PeersClient> p2PClients)
    {
        foreach (var item in p2PClients)
        {
            await item.DisposeAsync();
        }
    }

    private static (PeersClient peersClient, Channel<IPEndPoint> channel)[] CreatePeersClients(
        int number,
        ReadOnlyMemory<byte> infoHash,
        ILogger logger
    ) =>
        ValueEnumerable
            .Range(0, number)
            .Select(i =>
            {
                var channel = Channel.CreateUnbounded<IPEndPoint>();
                return (CreatePeersClient(new(), infoHash, channel, logger), channel);
            })
            .ToArray();

    private static async Task StartAsync(
        IEnumerable<PeersClient> p2pClients,
        CancellationToken cancellationToken
    )
    {
        foreach (var item in p2pClients)
        {
            _ = item.StartAsync(cancellationToken);
        }
    }

    private static PeersClient CreatePeersClient(
        PeerId peerId,
        ReadOnlyMemory<byte> infoHash,
        ChannelReader<IPEndPoint> channel,
        ILogger logger
    ) =>
        new(
            infoHash,
            peerId,
            new FakeRequestScheduler(),
            new FakeUploadScheduler(),
            new FakePiecePicker(),
            new Bitfield(5),
            channel,
            logger,
            null
        );

    private static PeersListener CreatePeersListener(PeerId peerId, ILogger logger) =>
        new(peerId, logger);
}
