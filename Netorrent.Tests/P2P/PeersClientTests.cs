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
        var logger = NullLogger.Instance;
        await using var peersClient = CreatePeersClient(new PeerId(), logger);
        var peersClients = CreatePeersClients(number, logger);
        var peerEndpointsObservable = peersClient.PeerConnected.Take(number);
        var p2pTask = peersClient.StartAsync(cts.Token);

        await StartAsync(peersClients, cts.Token);
        await ConnectPeersAsync(peersClients, peersClient, cancellationToken);
        var peerEndpointsCount = await peerEndpointsObservable.CountAsync(cts.Token);
        cts.Cancel();
        await DisposeP2pClients(peersClients);

        peerEndpointsCount.ShouldBe(number);
        await p2pTask.ShouldThrowAsync<OperationCanceledException>();
    }

    private static async Task ConnectPeersAsync(
        IEnumerable<PeersClient> peersClients,
        PeersClient listenerPeersClient,
        CancellationToken cancellationToken
    )
    {
        foreach (var peersClient in peersClients)
        {
            var incoming = Channel.CreateUnbounded<Message>();
            var outgoing = Channel.CreateUnbounded<Message>();
            var listenerPeer = new FakePeer(
                listenerPeersClient.PeerId,
                new IPEndPoint(IPAddress.Loopback, Random.Shared.Next(1024, 65535)),
                incoming,
                outgoing
            );
            var peer = new FakePeer(
                peersClient.PeerId,
                new IPEndPoint(IPAddress.Loopback, Random.Shared.Next(1024, 65535)),
                outgoing,
                incoming
            );
            await peersClient.AddPeerAsync(listenerPeer, cancellationToken).ConfigureAwait(false);
            await listenerPeersClient.AddPeerAsync(peer, cancellationToken).ConfigureAwait(false);
        }
    }

    private static async ValueTask DisposeP2pClients(IEnumerable<PeersClient> p2PClients)
    {
        foreach (var item in p2PClients)
        {
            await item.DisposeAsync();
        }
    }

    private static PeersClient[] CreatePeersClients(int number, ILogger logger) =>
        ValueEnumerable.Range(0, number).Select(i => CreatePeersClient(new(), logger)).ToArray();

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

    private static PeersClient CreatePeersClient(PeerId peerId, ILogger logger) =>
        new(
            peerId,
            new FakeRequestScheduler(),
            new FakeUploadScheduler(),
            new FakePiecePicker(),
            new Bitfield(5),
            logger
        );
}
