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
        await using var peerConnection = new FakePeerConnection();
        await using var uploadScheduler = new UploadScheduler(
            new FakePieceStorage(),
            new Bitfield(5, true),
            new TransferStatistics(10),
            logger
        );
        var requestBlock = new RequestBlock(0, 0, 0);
        var amChokingTask = peerConnection.AmChoking.FirstAsync(cancellationToken);
        var blockTask = peerConnection.SentBlocks.FirstAsync(cancellationToken);
        requestBlock.RequestedFrom.Add(peerConnection);
        await uploadScheduler.RequestSlotAsync(peerConnection, cancellationToken);
        await uploadScheduler.AddRequestAsync(requestBlock, cancellationToken);
        var uploadTask = uploadScheduler.StartAsync(cancellationToken);
        var block = await blockTask;
        block.Index.ShouldBe(0);
        block.Begin.ShouldBe(0);
        block.Payload.Length.ShouldBe(0);
        (await amChokingTask).ShouldBe(true);
    }
}
