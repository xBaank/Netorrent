using System.Net;
using System.Threading.Channels;
using Netorrent.P2P;
using Netorrent.P2P.Download;
using Netorrent.P2P.Messages;
using Netorrent.Tests.Fakes;
using R3;
using Shouldly;

namespace Netorrent.Tests.P2P;

[Timeout(5_000)]
public class PeerConectionTests
{
    [Test]
    public async Task Should_Receive_Unchoke(CancellationToken cancellationToken)
    {
        var ipEndpoint = new IPEndPoint(IPAddress.Loopback, 6881);
        var otherPeerId = new PeerId();
        var messages = Channel.CreateUnbounded<Message>();
        var bitfield = new Bitfield(10, true);

        await using var peerConnection = CreatePeerConnection(
            ipEndpoint,
            otherPeerId,
            messages,
            bitfield
        );

        _ = peerConnection.StartAsync(cancellationToken);
        var statesTask = peerConnection.PeerChoking.Take(2).ToListAsync(cancellationToken);
        await messages.Writer.WriteAsync(Message.CreateUnchoke(), cancellationToken);
        var states = await statesTask;
        peerConnection.PeerEndpoint.EndPoint.ShouldBe(ipEndpoint);
        peerConnection.PeerEndpoint.PeerId.ShouldBe(otherPeerId);
        states.First().ShouldBeTrue();
        states.Last().ShouldBeFalse();
    }

    [Test]
    public async Task Should_Receive_Interested(CancellationToken cancellationToken)
    {
        var ipEndpoint = new IPEndPoint(IPAddress.Loopback, 6881);
        var otherPeerId = new PeerId();
        var messages = Channel.CreateUnbounded<Message>();
        var bitfield = new Bitfield(10, true);

        await using var peerConnection = CreatePeerConnection(
            ipEndpoint,
            otherPeerId,
            messages,
            bitfield
        );

        _ = peerConnection.StartAsync(cancellationToken);
        var statesTask = peerConnection.PeerInterested.Take(2).ToListAsync(cancellationToken);
        await messages.Writer.WriteAsync(Message.CreateInterested(), cancellationToken);
        var states = await statesTask;
        peerConnection.PeerEndpoint.EndPoint.ShouldBe(ipEndpoint);
        peerConnection.PeerEndpoint.PeerId.ShouldBe(otherPeerId);
        states.First().ShouldBeFalse();
        states.Last().ShouldBeTrue();
    }

    private static PeerConnection CreatePeerConnection(
        IPEndPoint ipEndpoint,
        PeerId otherPeerId,
        Channel<Message> messages,
        Bitfield bitfield
    ) =>
        new(
            new PeerEndpoint(ipEndpoint, otherPeerId),
            bitfield,
            new FakeUploadScheduler(),
            new FakeRequestScheduler(),
            new FakeMessageStream(otherPeerId, messages, messages),
            new PeerRequestWindow(16 * 1024),
            new FakePiecePicker()
        );
}
