using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P;
using Netorrent.P2P.Messages;
using NSubstitute;
using NSubstitute.ReceivedExtensions;
using Shouldly;

namespace Netorrent.Tests.P2P;

[Timeout(5_000)]
public class PeerConectionTests
{
    [Test]
    public async Task Should_Receive_Unchoke(CancellationToken token)
    {
        await using var ctx = new PeerConnectionTestContext(peerChocking: true);

        _ = ctx.StartAsync(token);

        var stateChanged = ctx.Peer.NextStateAsync(token);
        await ctx.WriteAsync(Message.CreateUnchoke(), token);
        await stateChanged;

        ctx.Peer.PeerChocking.ShouldBeFalse();
        await ctx
            .RequestMock.Received()
            .OnPeerUnchockedAsync(Arg.Any<PeerConnection>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Should_Receive_Chocke(CancellationToken token)
    {
        await using var ctx = new PeerConnectionTestContext(peerChocking: false);

        _ = ctx.StartAsync(token);

        var state = ctx.Peer.NextStateAsync(token);
        await ctx.WriteAsync(Message.CreateChoke(), token);
        var bitfieldMessage = await ctx.ReadAsync(token);
        await state;

        ctx.Peer.PeerChocking.ShouldBeTrue();
        bitfieldMessage.Id.ShouldBe(Message.Bitfield);
    }

    [Test]
    public async Task Should_Receive_Interested_And_Unchoke(CancellationToken token)
    {
        var ctx = new PeerConnectionTestContext(amChocking: true, peerInterested: false);

        ctx.UploadMock.AddChokedSlot().Returns(true);
        ctx.UploadMock.RemoveChokedSlot().Returns(true);

        _ = ctx.StartAsync(token);

        var state = ctx.Peer.StateChanged.Take(2).ToList().ToTask(token);

        await ctx.WriteAsync(Message.CreateInterested(), token);

        var bitfieldMessage = await ctx.ReadAsync(token);
        var unchokeMessage = await ctx.ReadAsync(token);

        await state;
        await ctx.DisposeAsync();

        ctx.Peer.PeerInterested.ShouldBeTrue();
        ctx.Peer.AmChocking.ShouldBeFalse();
        bitfieldMessage.Id.ShouldBe(Message.Bitfield);
        unchokeMessage.Id.ShouldBe(Message.Unchoke);

        ctx.UploadMock.Received().AddChokedSlot();
        ctx.UploadMock.Received().RemoveChokedSlot();
    }

    [Test]
    public async Task Should_Receive_NotInterested_And_Choke(CancellationToken token)
    {
        await using var ctx = new PeerConnectionTestContext(
            peerInterested: true,
            amChocking: false
        );

        _ = ctx.StartAsync(token);

        var state = ctx.Peer.StateChanged.Take(2).ToList().ToTask(token);

        await ctx.WriteAsync(Message.CreateNotInterested(), token);

        var bitfieldMessage = await ctx.ReadAsync(token);
        var chokeMessage = await ctx.ReadAsync(token);

        await state;

        ctx.Peer.PeerInterested.ShouldBeFalse();
        ctx.Peer.AmChocking.ShouldBeTrue();
        bitfieldMessage.Id.ShouldBe(Message.Bitfield);
        chokeMessage.Id.ShouldBe(Message.Choke);
    }

    [Test]
    public async Task Should_Receive_Bitfield_And_Show_Interest(CancellationToken token)
    {
        var bitfield = new Bitfield(5);
        var peerBitfield = new Bitfield(5, true);

        var ctx = new PeerConnectionTestContext(bitfield);

        _ = ctx.StartAsync(token);

        // Expect IncreaseRarity called for each piece the peer has
        // (peerBitfield is all-true => length 5)
        await ctx.WriteAsync(Message.CreateBitfield(peerBitfield.ToRentedArray()), token);

        var bitfieldMessage = await ctx.ReadAsync(token);
        var interestedMessage = await ctx.ReadAsync(token);
        await ctx.DisposeAsync();

        // Verify internal state & outgoing messages
        ctx.Peer.AmInterested.ShouldBeTrue();
        bitfieldMessage.Id.ShouldBe(Message.Bitfield);
        interestedMessage.Id.ShouldBe(Message.Interested);

        ctx.RequestMock.Received(5).IncreaseRarity(Arg.Any<int>());
        ctx.RequestMock.Received(5).DecreaseRarity(Arg.Any<int>());
    }

    [Test]
    public async Task Should_Receive_Have_And_Show_Interest(CancellationToken token)
    {
        var ctx = new PeerConnectionTestContext(new Bitfield(5));

        _ = ctx.StartAsync(token);

        var callTask = ctx.RequestMock.WaitForCallAsync(
            x => x.IncreaseRarity(Arg.Any<int>()),
            token
        );
        await ctx.WriteAsync(Message.CreateHave(1), token);

        var bitfieldMessage = await ctx.ReadAsync(token);
        var interestedMessage = await ctx.ReadAsync(token);
        await callTask;
        await ctx.DisposeAsync();

        ctx.Peer.AmInterested.ShouldBeTrue();
        bitfieldMessage.Id.ShouldBe(Message.Bitfield);
        interestedMessage.Id.ShouldBe(Message.Interested);

        ctx.RequestMock.Received(1).IncreaseRarity(Arg.Is(1));
        ctx.RequestMock.Received(1).DecreaseRarity(Arg.Is(1));
    }

    [Test]
    public async Task Should_Receive_Have_And_Not_Show_Interest(CancellationToken token)
    {
        var local = new Bitfield(5);
        var ctx = new PeerConnectionTestContext(local);

        // mark local bitfield as already having piece 1
        local.SetPiece(1, token);

        _ = ctx.StartAsync(token);

        var callTask = ctx.RequestMock.WaitForCallAsync(
            x => x.IncreaseRarity(Arg.Any<int>()),
            token
        );
        await ctx.WriteAsync(Message.CreateHave(1), token);
        var bitfieldMessage = await ctx.ReadAsync(token);
        await callTask;
        await ctx.DisposeAsync();

        ctx.Peer.AmInterested.ShouldBeFalse();
        bitfieldMessage.Id.ShouldBe(Message.Bitfield);

        ctx.RequestMock.Received(1).IncreaseRarity(Arg.Is(1));
        ctx.RequestMock.Received(1).DecreaseRarity(Arg.Is(1));
    }

    [Test]
    public async Task Should_Receive_Bitfield_And_Not_Show_Interest(CancellationToken token)
    {
        // local has all pieces; peer has only piece 1 (so no interest)
        var local = new Bitfield(5, true);
        var peerBitfield = new Bitfield(5);
        peerBitfield.SetPiece(1, token);

        var ctx = new PeerConnectionTestContext(local);

        _ = ctx.StartAsync(token);

        await ctx.WriteAsync(Message.CreateBitfield(peerBitfield.ToRentedArray()), token);

        var bitfieldMessage = await ctx.ReadAsync(token);
        await ctx.DisposeAsync();

        ctx.Peer.AmInterested.ShouldBeFalse();
        bitfieldMessage.Id.ShouldBe(Message.Bitfield);

        ctx.RequestMock.Received(1).IncreaseRarity(Arg.Any<int>());
        ctx.RequestMock.Received(1).DecreaseRarity(Arg.Any<int>());
    }

    [Test]
    public async Task Should_Receive_Request(CancellationToken token)
    {
        var local = new Bitfield(5, true);
        var peerBitfield = new Bitfield(5);

        var ctx = new PeerConnectionTestContext(local);

        // Configure upload scheduler behavior
        ctx.UploadMock.AddRequestAsync(Arg.Any<RequestBlock>(), Arg.Any<CancellationToken>())
            .Returns(true);
        ctx.UploadMock.AddChokedSlot().Returns(true);
        ctx.UploadMock.RemoveChokedSlot().Returns(true);

        _ = ctx.StartAsync(token);

        var state = ctx.Peer.StateChanged.Take(2).ToList().ToTask(token);
        var addRequestCalled = ctx.UploadMock.WaitForCallAsync(
            async i =>
                await i.AddRequestAsync(Arg.Any<RequestBlock>(), Arg.Any<CancellationToken>()),
            token
        );

        await ctx.WriteAsync(Message.CreateBitfield(peerBitfield.ToRentedArray()), token);
        await ctx.WriteAsync(Message.CreateInterested(), token);

        var bitfieldMessage = await ctx.ReadAsync(token);
        var unchokeMessage = await ctx.ReadAsync(token);

        await ctx.WriteAsync(Message.CreateRequest(0, 0, FileManager.BlockSize), token);

        await state;
        await addRequestCalled;
        await ctx.DisposeAsync();

        ctx.Peer.PeerInterested.ShouldBeTrue();
        ctx.Peer.AmChocking.ShouldBeFalse();

        bitfieldMessage.Id.ShouldBe(Message.Bitfield);
        unchokeMessage.Id.ShouldBe(Message.Unchoke);

        await ctx
            .UploadMock.Received(1)
            .AddRequestAsync(Arg.Any<RequestBlock>(), Arg.Any<CancellationToken>());
        ctx.UploadMock.Received(1).AddChokedSlot();
        ctx.UploadMock.Received(1).RemoveChokedSlot();
        ctx.Peer.UploadRequestedBlocksCount.ShouldBe(1);
    }
}
