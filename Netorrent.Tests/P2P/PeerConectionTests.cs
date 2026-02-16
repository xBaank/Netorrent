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
    const int Port = 6881;

    static IPEndPoint Endpoint => new(IPAddress.Loopback, Port);
    static PeerId OtherPeerId => new();

    static async Task<(
        PeerConnection PeerConnection,
        Channel<Message> Incoming,
        Channel<Message> Outgoing
    )> CreateTestContextAsync(bool bitfieldFull)
    {
        var incoming = Channel.CreateUnbounded<Message>();
        var outgoing = Channel.CreateUnbounded<Message>();
        var bitfield = new Bitfield(10, bitfieldFull);
        var peerConnection = CreatePeerConnection(
            Endpoint,
            OtherPeerId,
            incoming,
            outgoing,
            bitfield
        );
        return (peerConnection, incoming, outgoing);
    }

    private static PeerConnection CreatePeerConnection(
        IPEndPoint ipEndpoint,
        PeerId otherPeerId,
        Channel<Message> incommingMessages,
        Channel<Message> outgoingMessages,
        Bitfield bitfield
    ) =>
        new(
            new PeerEndpoint(ipEndpoint, otherPeerId),
            bitfield,
            new FakeUploadScheduler(),
            new FakeRequestScheduler(),
            new FakeMessageStream(otherPeerId, incommingMessages, outgoingMessages),
            new PeerRequestWindow(16 * 1024),
            new PiecePicker(bitfield, 16 * 1024, 256 * 1024, 256 * 1024)
        );

    [Test]
    public async Task Should_Receive_Unchoke(CancellationToken cancellationToken)
    {
        var (peerConnection, incoming, _) = await CreateTestContextAsync(true);
        await using var __ = peerConnection;

        _ = peerConnection.StartAsync(cancellationToken);
        var statesTask = peerConnection.PeerChoking.Take(2).ToListAsync(cancellationToken);
        await incoming.Writer.WriteAsync(Message.CreateUnchoke(), cancellationToken);
        var states = await statesTask;

        states.First().ShouldBeTrue();
        states.Last().ShouldBeFalse();
    }

    [Test]
    public async Task Should_Send_Unchoke(CancellationToken cancellationToken)
    {
        var (peerConnection, _, _) = await CreateTestContextAsync(true);
        await using var __ = peerConnection;

        _ = peerConnection.StartAsync(cancellationToken);
        var statesTask = peerConnection.AmChoking.Take(2).ToListAsync(cancellationToken);

        peerConnection.Unchoke();
        var states = await statesTask;

        states.First().ShouldBeTrue();
        states.Last().ShouldBeFalse();
    }

    [Test]
    public async Task Should_Receive_Interested(CancellationToken cancellationToken)
    {
        var (peerConnection, incoming, _) = await CreateTestContextAsync(true);
        await using var __ = peerConnection;

        _ = peerConnection.StartAsync(cancellationToken);
        var statesTask = peerConnection.PeerInterested.Take(2).ToListAsync(cancellationToken);
        await incoming.Writer.WriteAsync(Message.CreateInterested(), cancellationToken);
        var states = await statesTask;

        states.First().ShouldBeFalse();
        states.Last().ShouldBeTrue();
    }

    [Test]
    public async Task Should_Send_Interested(CancellationToken cancellationToken)
    {
        var (peerConnection, incoming, _) = await CreateTestContextAsync(false);
        await using var __ = peerConnection;

        _ = peerConnection.StartAsync(cancellationToken);
        var statesTask = peerConnection.AmInterested.Take(2).ToListAsync(cancellationToken);
        await incoming.Writer.WriteAsync(Message.CreateHave(5), cancellationToken);
        var states = await statesTask;

        states.First().ShouldBeFalse();
        states.Last().ShouldBeTrue();
    }
}
