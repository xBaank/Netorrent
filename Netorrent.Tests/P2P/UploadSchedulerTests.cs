using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Netorrent.P2P;
using Netorrent.P2P.Messages;
using Netorrent.P2P.Upload;
using Netorrent.Statistics;
using R3;
using Shouldly;

namespace Netorrent.Tests.P2P;

[Timeout(10_000)]
public class UploadSchedulerTests
{
    const int PieceCount = 5;
    const int BlockSize = 1024 * 4;

    static ILogger Logger => NullLogger.Instance;

    static async Task<(
        FakePeerConnection Peer,
        UploadScheduler Scheduler
    )> CreateUploadSchedulerAsync(CancellationToken cancellationToken)
    {
        var bitfield = new Bitfield(PieceCount, true);
        var leecherBitfield = new Bitfield(PieceCount, false);
        var peer = new FakePeerConnection(bitfield, leecherBitfield, BlockSize);
        peer.PeerInterested.Value = true;

        var scheduler = new UploadScheduler(
            new Dictionary<PeerEndpoint, IPeerConnection> { [peer.PeerEndpoint] = peer },
            new FakePieceStorage(),
            bitfield,
            new DataStatistics(10),
            Logger
        );

        return (peer, scheduler);
    }

    [Test]
    public async Task Should_Upload_Block_To_Peer(CancellationToken cancellationToken)
    {
        var (peer, scheduler) = await CreateUploadSchedulerAsync(cancellationToken);
        await using var __ = scheduler;

        var requestBlock = new RequestBlock(0, 0, 0);
        var amChokingTask = peer.AmChoking.FirstAsync(i => i == false, cancellationToken);
        var blockTask = peer.SentBlocks.FirstAsync(cancellationToken);
        requestBlock.RequestedFrom.Add(peer);
        _ = scheduler.StartAsync(cancellationToken);

        var chokeState = await amChokingTask;
        await scheduler.AddRequestAsync(requestBlock, cancellationToken);
        var block = await blockTask;

        block.Index.ShouldBe(0);
        block.Begin.ShouldBe(0);
        block.Payload.Length.ShouldBe(0);
        chokeState.ShouldBe(false);
    }

    [Test]
    public async Task Should_Cancel_Block_To_Peer(CancellationToken cancellationToken)
    {
        var (peer, scheduler) = await CreateUploadSchedulerAsync(cancellationToken);
        await using var __ = scheduler;

        var requestBlock = new RequestBlock(0, 0, 0);
        var amChokingTask = peer.AmChoking.FirstAsync(i => i == false, cancellationToken);
        var isEmptyTask = peer.SentBlocks.IsEmptyAsync(cancellationToken);
        requestBlock.RequestedFrom.Add(peer);
        _ = scheduler.StartAsync(cancellationToken);

        var chokeState = await amChokingTask;
        await scheduler.AddRequestAsync(requestBlock, cancellationToken);
        scheduler.CancelRequest(requestBlock);
        await Task.Delay(1000, cancellationToken);
        await peer.DisposeAsync();

        (await isEmptyTask).ShouldBe(true);
        chokeState.ShouldBe(false);
    }
}
