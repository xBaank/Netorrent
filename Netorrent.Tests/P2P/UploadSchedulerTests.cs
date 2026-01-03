using Microsoft.Extensions.Logging.Abstractions;
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
    public async Task Should_Upload_To_Peer(CancellationToken cancellationToken)
    {
        var logger = NullLogger.Instance;
        var bitfield = new Bitfield(5, true);
        await using var peerConnection = new FakePeerConnection(bitfield);
        await using var uploadScheduler = new UploadScheduler(
            new FakePieceStorage(),
            bitfield,
            new TransferStatistics(10),
            logger
        );
        var requestBlock = new RequestBlock(0, 0, 0);
        var amChokingTask = peerConnection.AmChoking.FirstAsync(i => i == false, cancellationToken);
        var blockTask = peerConnection.SentBlocks.FirstAsync(cancellationToken);
        peerConnection.PeerInterested.Value = true;
        requestBlock.RequestedFrom.Add(peerConnection);
        uploadScheduler.AddPeer(peerConnection);
        var uploadTask = uploadScheduler.StartAsync(cancellationToken);
        var chokeState = await amChokingTask;
        await uploadScheduler.AddRequestAsync(requestBlock, cancellationToken);
        var block = await blockTask;
        block.Index.ShouldBe(0);
        block.Begin.ShouldBe(0);
        block.Payload.Length.ShouldBe(0);
        chokeState.ShouldBe(false);
    }
}
