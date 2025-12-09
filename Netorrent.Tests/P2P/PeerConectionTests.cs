using System.Buffers;
using System.Reactive.Linq;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.Other;
using Netorrent.P2P;
using Netorrent.P2P.Messages;
using Netorrent.Tests.Extensions;
using NSubstitute;
using NSubstitute.ReceivedExtensions;
using Shouldly;

namespace Netorrent.Tests.P2P;

[Timeout(5_000)]
public class PeerConectionTests
{
    //These test cases simulate other peer sending data through a channel.
    [Test]
    public async Task Should_Receive_Unchoke(CancellationToken token)
    {
        await using var ctx = new PeerConnectionTestContext(peerChocking: true);
        var stateChanged = ctx.Peer.NextStateAsync(token);

        _ = ctx.StartAsync(token);

        await ctx.WriteAsync(Message.CreateUnchoke(), token);
        using var bitfieldMessage = await ctx.ReadAsync(token);

        await stateChanged;

        bitfieldMessage.Id.ShouldBe(Message.Bitfield);
        ctx.Peer.PeerChoking.ShouldBeFalse();
    }

    [Test]
    public async Task Should_Receive_Chocke(CancellationToken token)
    {
        await using var ctx = new PeerConnectionTestContext(peerChocking: false);

        _ = ctx.StartAsync(token);

        var state = ctx.Peer.NextStateAsync(token);
        await ctx.WriteAsync(Message.CreateChoke(), token);
        using var bitfieldMessage = await ctx.ReadAsync(token);
        await state;

        ctx.Peer.PeerChoking.ShouldBeTrue();
        bitfieldMessage.Id.ShouldBe(Message.Bitfield);
    }

    [Test]
    public async Task Should_Receive_Interested_And_Unchoke(CancellationToken token)
    {
        var ctx = new PeerConnectionTestContext(amChocking: true, peerInterested: false);
        var state = ctx.Peer.NextStateAsync(2, token);

        _ = ctx.StartAsync(token);

        await ctx.WriteAsync(Message.CreateInterested(), token);

        using var bitfieldMessage = await ctx.ReadAsync(token);
        await ctx.Peer.SendUnchokedAsync(token);
        using var unchokeMessage = await ctx.ReadAsync(token);

        await state;
        await ctx.DisposeAsync();

        ctx.Peer.PeerInterested.ShouldBeTrue();
        ctx.Peer.AmChoking.ShouldBeFalse();
        bitfieldMessage.Id.ShouldBe(Message.Bitfield);
        unchokeMessage.Id.ShouldBe(Message.Unchoke);

        await ctx
            .UploadMock.Received()
            .RequestSlotAsync(Arg.Any<PeerConnection>(), Arg.Any<CancellationToken>());
        await ctx
            .UploadMock.Received()
            .FreeSlotAsync(Arg.Any<PeerConnection>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Should_Receive_NotInterested_And_Choke(CancellationToken token)
    {
        await using var ctx = new PeerConnectionTestContext(
            peerInterested: true,
            amChocking: false
        );
        var state = ctx.Peer.NextStateAsync(2, token);

        _ = ctx.StartAsync(token);

        await ctx.WriteAsync(Message.CreateNotInterested(), token);

        using var bitfieldMessage = await ctx.ReadAsync(token);
        using var chokeMessage = await ctx.ReadAsync(token);

        await state;

        ctx.Peer.PeerInterested.ShouldBeFalse();
        ctx.Peer.AmChoking.ShouldBeTrue();
        bitfieldMessage.Id.ShouldBe(Message.Bitfield);
        chokeMessage.Id.ShouldBe(Message.Choke);
    }

    [Test]
    public async Task Should_Receive_Bitfield_And_Show_Interest(CancellationToken token)
    {
        var bitfield = new Bitfield(5);
        var peerBitfield = new Bitfield(5, true);

        var ctx = new PeerConnectionTestContext(bitfield);
        var callTask = ctx.RequestMock.WaitForCallAsync(
            x => x.IncreaseRarity(Arg.Any<int>()),
            token
        );

        _ = ctx.StartAsync(token);

        await ctx.WriteAsync(Message.CreateBitfield(peerBitfield.ToRentedArray()), token);

        using var bitfieldMessage = await ctx.ReadAsync(token);
        using var interestedMessage = await ctx.ReadAsync(token);
        await callTask;
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
        var callTask = ctx.RequestMock.WaitForCallAsync(
            x => x.IncreaseRarity(Arg.Any<int>()),
            token
        );

        _ = ctx.StartAsync(token);

        await ctx.WriteAsync(Message.CreateHave(1), token);

        using var bitfieldMessage = await ctx.ReadAsync(token);
        using var interestedMessage = await ctx.ReadAsync(token);
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
        local.SetPiece(1);
        var callTask = ctx.RequestMock.WaitForCallAsync(
            x => x.IncreaseRarity(Arg.Any<int>()),
            token
        );

        _ = ctx.StartAsync(token);

        await ctx.WriteAsync(Message.CreateHave(1), token);
        using var bitfieldMessage = await ctx.ReadAsync(token);
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
        peerBitfield.SetPiece(1);

        var ctx = new PeerConnectionTestContext(local);
        var callTask = ctx.RequestMock.WaitForCallAsync(
            x => x.IncreaseRarity(Arg.Any<int>()),
            token
        );

        _ = ctx.StartAsync(token);

        await ctx.WriteAsync(Message.CreateBitfield(peerBitfield.ToRentedArray()), token);

        using var bitfieldMessage = await ctx.ReadAsync(token);
        await callTask;
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
        ctx.UploadMock.RequestSlotAsync(Arg.Any<PeerConnection>(), Arg.Any<CancellationToken>())
            .Returns(ValueTask.CompletedTask);
        ctx.UploadMock.FreeSlotAsync(Arg.Any<PeerConnection>(), Arg.Any<CancellationToken>())
            .Returns(ValueTask.CompletedTask);
        var state = ctx.Peer.NextStateAsync(2, token);
        var addRequestCalled = ctx.UploadMock.WaitForCallAsync(
            async i =>
                await i.AddRequestAsync(Arg.Any<RequestBlock>(), Arg.Any<CancellationToken>()),
            token
        );

        _ = ctx.StartAsync(token);

        await ctx.WriteAsync(Message.CreateBitfield(peerBitfield.ToRentedArray()), token);
        await ctx.WriteAsync(Message.CreateInterested(), token);

        using var bitfieldMessage = await ctx.ReadAsync(token);
        await ctx.Peer.SendUnchokedAsync(token);
        using var unchokeMessage = await ctx.ReadAsync(token);

        await ctx.WriteAsync(Message.CreateRequest(0, 0, FileManager.BlockSize), token);

        await state;
        await addRequestCalled;
        await ctx.DisposeAsync();

        ctx.Peer.PeerInterested.ShouldBeTrue();
        ctx.Peer.AmChoking.ShouldBeFalse();

        bitfieldMessage.Id.ShouldBe(Message.Bitfield);
        unchokeMessage.Id.ShouldBe(Message.Unchoke);

        await ctx
            .UploadMock.Received(1)
            .AddRequestAsync(Arg.Any<RequestBlock>(), Arg.Any<CancellationToken>());
        await ctx
            .UploadMock.Received(1)
            .RequestSlotAsync(Arg.Any<PeerConnection>(), Arg.Any<CancellationToken>());
        await ctx
            .UploadMock.Received(1)
            .FreeSlotAsync(Arg.Any<PeerConnection>(), Arg.Any<CancellationToken>());
        ctx.Peer.UploadRequestedBlocksCount.ShouldBe(1);
    }

    [Test]
    public async Task Should_Receive_Block(CancellationToken token)
    {
        var local = new Bitfield(5);
        var peerBitfield = new Bitfield(5, true);
        Block? capturedBlock = null;

        var ctx = new PeerConnectionTestContext(local);

        var state = ctx.Peer.NextStateAsync(2, token);
        ctx.RequestMock.When(async x =>
                await x.ReceiveBlockAsync(Arg.Any<Block>(), Arg.Any<CancellationToken>())
            )
            .Do(callInfo =>
            {
                capturedBlock = callInfo.Arg<Block>();
            });
        var receiveBlockCalled = ctx.RequestMock.WaitForCallAsync(
            async i => await i.ReceiveBlockAsync(Arg.Any<Block>(), Arg.Any<CancellationToken>()),
            token
        );

        using var rentedArray = new RentedArray<byte>(ArrayPool<byte>.Shared.Rent(1), 1);

        _ = ctx.StartAsync(token);

        await ctx.WriteAsync(Message.CreateBitfield(peerBitfield.ToRentedArray()), token);
        await ctx.WriteAsync(Message.CreateUnchoke(), token);
        await ctx.WriteAsync(Message.CreatePiece(0, 0, rentedArray), token);

        using var bitfieldMessage = await ctx.ReadAsync(token);
        using var intersetedMessage = await ctx.ReadAsync(token);

        await state;
        await receiveBlockCalled;
        capturedBlock?.Dispose();
        await ctx.DisposeAsync();

        ctx.Peer.AmInterested.ShouldBeTrue();
        ctx.Peer.PeerChoking.ShouldBeFalse();

        bitfieldMessage.Id.ShouldBe(Message.Bitfield);
        intersetedMessage.Id.ShouldBe(Message.Interested);

        await ctx
            .RequestMock.Received(1)
            .ReceiveBlockAsync(Arg.Any<Block>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Should_Send_Request(CancellationToken token)
    {
        var local = new Bitfield(5);
        var peerBitfield = new Bitfield(5, true);

        var ctx = new PeerConnectionTestContext(local);

        var request = new RequestBlock(0, 0, 1)
        {
            RequestedAt = DateTimeOffset.UtcNow,
            State = RequestBlockState.Requested,
        };

        var state = ctx.Peer.NextStateAsync(2, token);

        _ = ctx.StartAsync(token);

        await ctx.WriteAsync(Message.CreateBitfield(peerBitfield.ToRentedArray()), token);
        await ctx.WriteAsync(Message.CreateUnchoke(), token);

        using var bitfieldMessage = await ctx.ReadAsync(token);
        using var interestedMessage = await ctx.ReadAsync(token);
        await ctx.Peer.SendRequestAsync(request, token);
        using var requestMessage = await ctx.ReadAsync(token);

        await state;
        await ctx.DisposeAsync();

        ctx.Peer.AmInterested.ShouldBeTrue();
        ctx.Peer.PeerChoking.ShouldBeFalse();

        bitfieldMessage.Id.ShouldBe(Message.Bitfield);
        interestedMessage.Id.ShouldBe(Message.Interested);
        requestMessage.Id.ShouldBe(Message.Request);
    }
}
