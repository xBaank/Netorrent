using System.Net;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using Netorrent.Extensions;
using Netorrent.P2P;
using Netorrent.P2P.Download;
using Netorrent.P2P.Messages;
using Netorrent.P2P.Upload;
using Netorrent.Tests.Fakes;
using Rocks;
using Shouldly;

[assembly: Rock(typeof(IRequestScheduler), BuildType.Create)]
[assembly: Rock(typeof(IUploadScheduler), BuildType.Create)]
[assembly: Rock(typeof(IRequestScheduler), BuildType.Make)]
[assembly: Rock(typeof(IUploadScheduler), BuildType.Make)]

namespace Netorrent.Tests.P2P;

[Timeout(5_000)]
public class PeerConectionTests
{
    [Test]
    public async Task Should_Receive_Unchocke(CancellationToken cancellationToken)
    {
        var bitfield = new Bitfield(5);
        var peerId = new PeerId();
        var incomingChannel = Channel.CreateUnbounded<Message>();
        var outgoingChannel = Channel.CreateUnbounded<Message>();
        var fakeMessageStream = new FakeMessageStream(peerId, incomingChannel, outgoingChannel);
        var mockUploadScheduler = new IUploadSchedulerMakeExpectations();
        var mockRequestScheduler = new IRequestSchedulerCreateExpectations();

        mockRequestScheduler
            .Methods.OnPeerUnchockedAsync(Arg.Any<PeerConnection>(), Arg.Any<CancellationToken>())
            .ExpectedCallCount(1);

        await using var peerConnection = new PeerConnection(
            new IPEndPoint(IPAddress.Loopback, 0),
            bitfield,
            mockUploadScheduler.Instance(),
            mockRequestScheduler.Instance(),
            fakeMessageStream,
            peerChocking: true
        );

        _ = peerConnection.StartAsync(cancellationToken);
        var state = peerConnection.StateChanged.FirstOrDefaultAsync().ToTask(cancellationToken);
        await incomingChannel.Writer.WriteAsync(Message.CreateUnchoke(), cancellationToken);
        await state;

        peerConnection.PeerChocking.ShouldBeFalse();
        mockRequestScheduler.Verify();
    }

    [Test]
    public async Task Should_Receive_Chocke(CancellationToken cancellationToken)
    {
        var bitfield = new Bitfield(5);
        var peerId = new PeerId();
        var incomingChannel = Channel.CreateUnbounded<Message>();
        var outgoingChannel = Channel.CreateUnbounded<Message>();
        var fakeMessageStream = new FakeMessageStream(peerId, incomingChannel, outgoingChannel);
        var mockUploadScheduler = new IUploadSchedulerMakeExpectations();
        var mockRequestScheduler = new IRequestSchedulerMakeExpectations();
        await using var peerConnection = new PeerConnection(
            new IPEndPoint(IPAddress.Loopback, 0),
            bitfield,
            mockUploadScheduler.Instance(),
            mockRequestScheduler.Instance(),
            fakeMessageStream,
            peerChocking: false
        );

        _ = peerConnection.StartAsync(cancellationToken);
        var state = peerConnection.StateChanged.FirstOrDefaultAsync().ToTask(cancellationToken);
        await incomingChannel.Writer.WriteAsync(Message.CreateChoke(), cancellationToken);
        var bitfieldMessage = await outgoingChannel.Reader.ReadAsync(cancellationToken);
        await state;

        peerConnection.PeerChocking.ShouldBeTrue();
        bitfieldMessage.Id.ShouldBe(Message.Bitfield);
    }

    [Test]
    public async Task Should_Receive_Interested_And_Unchoke(CancellationToken cancellationToken)
    {
        var bitfield = new Bitfield(5);
        var peerId = new PeerId();
        var incomingChannel = Channel.CreateUnbounded<Message>();
        var outgoingChannel = Channel.CreateUnbounded<Message>();
        var fakeMessageStream = new FakeMessageStream(peerId, incomingChannel, outgoingChannel);
        var mockUploadScheduler = new IUploadSchedulerCreateExpectations();
        var mockRequestScheduler = new IRequestSchedulerMakeExpectations();
        mockUploadScheduler.Methods.AddChokedSlot().ReturnValue(true).ExpectedCallCount(1);
        mockUploadScheduler.Methods.RemoveChokedSlot().ReturnValue(true).ExpectedCallCount(1);
        var peerConnection = new PeerConnection(
            new IPEndPoint(IPAddress.Loopback, 0),
            bitfield,
            mockUploadScheduler.Instance(),
            mockRequestScheduler.Instance(),
            fakeMessageStream,
            peerInterested: false,
            amChocking: true
        );

        _ = peerConnection.StartAsync(cancellationToken);
        var state = peerConnection.StateChanged.Take(2).ToList().ToTask(cancellationToken);
        await incomingChannel.Writer.WriteAsync(Message.CreateInterested(), cancellationToken);
        var bitfieldMessage = await outgoingChannel.Reader.ReadAsync(cancellationToken);
        var unchokeMessage = await outgoingChannel.Reader.ReadAsync(cancellationToken);
        await state;
        await peerConnection.DisposeAsync();

        peerConnection.PeerInterested.ShouldBeTrue();
        peerConnection.AmChocking.ShouldBeFalse();
        bitfieldMessage.Id.ShouldBe(Message.Bitfield);
        unchokeMessage.Id.ShouldBe(Message.Unchoke);
        mockUploadScheduler.Verify();
    }

    [Test]
    public async Task Should_Receive_NotInterested_And_Choke(CancellationToken cancellationToken)
    {
        var bitfield = new Bitfield(5);
        var peerId = new PeerId();
        var incomingChannel = Channel.CreateUnbounded<Message>();
        var outgoingChannel = Channel.CreateUnbounded<Message>();
        var fakeMessageStream = new FakeMessageStream(peerId, incomingChannel, outgoingChannel);
        var mockUploadScheduler = new IUploadSchedulerMakeExpectations();
        var mockRequestScheduler = new IRequestSchedulerMakeExpectations();
        await using var peerConnection = new PeerConnection(
            new IPEndPoint(IPAddress.Loopback, 0),
            bitfield,
            mockUploadScheduler.Instance(),
            mockRequestScheduler.Instance(),
            fakeMessageStream,
            peerInterested: true,
            amChocking: false
        );

        _ = peerConnection.StartAsync(cancellationToken);
        var state = peerConnection.StateChanged.Take(2).ToList().ToTask(cancellationToken);
        await incomingChannel.Writer.WriteAsync(Message.CreateNotInterested(), cancellationToken);
        var bitfieldMessage = await outgoingChannel.Reader.ReadAsync(cancellationToken);
        var chokeMessage = await outgoingChannel.Reader.ReadAsync(cancellationToken);
        await state;

        peerConnection.PeerInterested.ShouldBeFalse();
        peerConnection.AmChocking.ShouldBeTrue();
        bitfieldMessage.Id.ShouldBe(Message.Bitfield);
        chokeMessage.Id.ShouldBe(Message.Choke);
    }

    [Test]
    public async Task Should_Receive_Bitfield_And_Show_Interest(CancellationToken cancellationToken)
    {
        var bitfield = new Bitfield(5);
        var peerBitfield = new Bitfield(5, true);
        var peerId = new PeerId();
        var incomingChannel = Channel.CreateUnbounded<Message>();
        var outgoingChannel = Channel.CreateUnbounded<Message>();
        var fakeMessageStream = new FakeMessageStream(peerId, incomingChannel, outgoingChannel);
        var mockUploadScheduler = new IUploadSchedulerMakeExpectations();
        var mockRequestScheduler = new IRequestSchedulerCreateExpectations();
        mockRequestScheduler.Methods.IncreaseRarity(Arg.Any<int>()).ExpectedCallCount(5);
        mockRequestScheduler.Methods.DecreaseRarity(Arg.Any<int>()).ExpectedCallCount(5);
        var peerConnection = new PeerConnection(
            new IPEndPoint(IPAddress.Loopback, 0),
            bitfield,
            mockUploadScheduler.Instance(),
            mockRequestScheduler.Instance(),
            fakeMessageStream
        );

        _ = peerConnection.StartAsync(cancellationToken);
        var state = peerConnection.StateChanged.FirstOrDefaultAsync().ToTask(cancellationToken);
        await incomingChannel.Writer.WriteAsync(
            Message.CreateBitfield(peerBitfield.ToRentedArray()),
            cancellationToken
        );
        var bitfieldMessage = await outgoingChannel.Reader.ReadAsync(cancellationToken);
        var interestedMessage = await outgoingChannel.Reader.ReadAsync(cancellationToken);
        await state;
        await peerConnection.DisposeAsync();

        peerConnection.AmInterested.ShouldBeTrue();
        bitfieldMessage.Id.ShouldBe(Message.Bitfield);
        interestedMessage.Id.ShouldBe(Message.Interested);
        mockRequestScheduler.Verify();
    }

    [Test]
    public async Task Should_Receive_Have_And_Show_Interest(CancellationToken cancellationToken)
    {
        var bitfield = new Bitfield(5);
        var peerId = new PeerId();
        var incomingChannel = Channel.CreateUnbounded<Message>();
        var outgoingChannel = Channel.CreateUnbounded<Message>();
        var fakeMessageStream = new FakeMessageStream(peerId, incomingChannel, outgoingChannel);
        var mockUploadScheduler = new IUploadSchedulerMakeExpectations();
        var mockRequestScheduler = new IRequestSchedulerCreateExpectations();
        mockRequestScheduler.Methods.IncreaseRarity(Arg.Is(1)).ExpectedCallCount(1);
        mockRequestScheduler.Methods.DecreaseRarity(Arg.Any<int>()).ExpectedCallCount(1);
        var peerConnection = new PeerConnection(
            new IPEndPoint(IPAddress.Loopback, 0),
            bitfield,
            mockUploadScheduler.Instance(),
            mockRequestScheduler.Instance(),
            fakeMessageStream
        );

        _ = peerConnection.StartAsync(cancellationToken);
        var state = peerConnection.StateChanged.FirstOrDefaultAsync().ToTask(cancellationToken);
        await incomingChannel.Writer.WriteAsync(Message.CreateHave(1), cancellationToken);
        var bitfieldMessage = await outgoingChannel.Reader.ReadAsync(cancellationToken);
        var interestedMessage = await outgoingChannel.Reader.ReadAsync(cancellationToken);
        await state;
        await peerConnection.DisposeAsync();

        peerConnection.AmInterested.ShouldBeTrue();
        bitfieldMessage.Id.ShouldBe(Message.Bitfield);
        interestedMessage.Id.ShouldBe(Message.Interested);
        mockRequestScheduler.Verify();
    }

    [Test]
    public async Task Should_Receive_Have_And_Not_Show_Interest(CancellationToken cancellationToken)
    {
        var bitfield = new Bitfield(5);
        bitfield.SetPiece(1, cancellationToken);
        var peerId = new PeerId();
        var incomingChannel = Channel.CreateUnbounded<Message>();
        var outgoingChannel = Channel.CreateUnbounded<Message>();
        var fakeMessageStream = new FakeMessageStream(peerId, incomingChannel, outgoingChannel);
        var mockUploadScheduler = new IUploadSchedulerMakeExpectations();
        var mockRequestScheduler = new IRequestSchedulerCreateExpectations();
        mockRequestScheduler.Methods.IncreaseRarity(Arg.Is(1)).ExpectedCallCount(1);
        mockRequestScheduler.Methods.DecreaseRarity(Arg.Any<int>()).ExpectedCallCount(1);
        var peerConnection = new PeerConnection(
            new IPEndPoint(IPAddress.Loopback, 0),
            bitfield,
            mockUploadScheduler.Instance(),
            mockRequestScheduler.Instance(),
            fakeMessageStream
        );

        _ = peerConnection.StartAsync(cancellationToken);
        await incomingChannel.Writer.WriteAsync(Message.CreateHave(1), cancellationToken);
        var bitfieldMessage = await outgoingChannel.Reader.ReadAsync(cancellationToken);
        await peerConnection.DisposeAsync();

        peerConnection.AmInterested.ShouldBeFalse();
        bitfieldMessage.Id.ShouldBe(Message.Bitfield);
        mockRequestScheduler.Verify();
    }

    [Test]
    public async Task Should_Receive_Bitfield_And_Not_Show_Interest(
        CancellationToken cancellationToken
    )
    {
        var bitfield = new Bitfield(5, true);
        var peerBitfield = new Bitfield(5);
        peerBitfield.SetPiece(1, cancellationToken);
        var peerId = new PeerId();
        var incomingChannel = Channel.CreateUnbounded<Message>();
        var outgoingChannel = Channel.CreateUnbounded<Message>();
        var fakeMessageStream = new FakeMessageStream(peerId, incomingChannel, outgoingChannel);
        var mockUploadScheduler = new IUploadSchedulerMakeExpectations();
        var mockRequestScheduler = new IRequestSchedulerCreateExpectations();
        mockRequestScheduler.Methods.IncreaseRarity(Arg.Any<int>()).ExpectedCallCount(1);
        mockRequestScheduler.Methods.DecreaseRarity(Arg.Any<int>()).ExpectedCallCount(1);
        var peerConnection = new PeerConnection(
            new IPEndPoint(IPAddress.Loopback, 0),
            bitfield,
            mockUploadScheduler.Instance(),
            mockRequestScheduler.Instance(),
            fakeMessageStream
        );

        _ = peerConnection.StartAsync(cancellationToken);
        await incomingChannel.Writer.WriteAsync(
            Message.CreateBitfield(peerBitfield.ToRentedArray()),
            cancellationToken
        );
        var bitfieldMessage = await outgoingChannel.Reader.ReadAsync(cancellationToken);
        await peerConnection.DisposeAsync();

        peerConnection.AmInterested.ShouldBeFalse();
        bitfieldMessage.Id.ShouldBe(Message.Bitfield);
        mockRequestScheduler.Verify();
    }
}
