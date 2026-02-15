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
    //TODO use matrix to create multiple peers
    [Test]
    public async Task Should_Upload_Block_To_Peer(CancellationToken cancellationToken)
    {
        var logger = NullLogger.Instance;
        var bitfield = new Bitfield(5, true);
        var leecherBitfield = new Bitfield(5, false);
        await using var leecherPeerConnection = new FakePeerConnection(
            bitfield,
            leecherBitfield,
            1024 * 4
        );
        leecherPeerConnection.PeerInterested.Value = true;
        await using var uploadScheduler = new UploadScheduler(
            new Dictionary<PeerEndpoint, IPeerConnection>()
            {
                [leecherPeerConnection.PeerEndpoint] = leecherPeerConnection,
            },
            new FakePieceStorage(),
            bitfield,
            new DataStatistics(10),
            logger
        );
        var requestBlock = new RequestBlock(0, 0, 0);
        var amChokingTask = leecherPeerConnection.AmChoking.FirstAsync(
            i => i == false,
            cancellationToken
        );
        var blockTask = leecherPeerConnection.SentBlocks.FirstAsync(cancellationToken);
        requestBlock.RequestedFrom.Add(leecherPeerConnection);
        var uploadTask = uploadScheduler.StartAsync(cancellationToken);
        var chokeState = await amChokingTask;
        await uploadScheduler.AddRequestAsync(requestBlock, cancellationToken);
        var block = await blockTask;
        block.Index.ShouldBe(0);
        block.Begin.ShouldBe(0);
        block.Payload.Length.ShouldBe(0);
        chokeState.ShouldBe(false);
    }

    [Test]
    public async Task Should_Cancel_Block_To_Peer(CancellationToken cancellationToken)
    {
        var logger = NullLogger.Instance;
        var bitfield = new Bitfield(5, true);
        var leecherBitfield = new Bitfield(5, false);
        var leecherPeerConnection = new FakePeerConnection(bitfield, leecherBitfield, 1024 * 4);
        leecherPeerConnection.PeerInterested.Value = true;
        await using var uploadScheduler = new UploadScheduler(
            new Dictionary<PeerEndpoint, IPeerConnection>()
            {
                [leecherPeerConnection.PeerEndpoint] = leecherPeerConnection,
            },
            new FakePieceStorage(),
            bitfield,
            new DataStatistics(10),
            logger
        );
        var requestBlock = new RequestBlock(0, 0, 0);
        var amChokingTask = leecherPeerConnection.AmChoking.FirstAsync(
            i => i == false,
            cancellationToken
        );
        var isEmptyTask = leecherPeerConnection.SentBlocks.IsEmptyAsync(cancellationToken);
        requestBlock.RequestedFrom.Add(leecherPeerConnection);
        var uploadTask = uploadScheduler.StartAsync(cancellationToken);
        var chokeState = await amChokingTask;
        await uploadScheduler.AddRequestAsync(requestBlock, cancellationToken);
        uploadScheduler.CancelRequest(requestBlock);
        await Task.Delay(1000, cancellationToken);
        await leecherPeerConnection.DisposeAsync();
        (await isEmptyTask).ShouldBe(true);
        chokeState.ShouldBe(false);
    }
}
