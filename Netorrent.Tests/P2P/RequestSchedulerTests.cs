using System.Buffers;
using Microsoft.Extensions.Logging.Abstractions;
using Netorrent.Other;
using Netorrent.P2P;
using Netorrent.P2P.Download;
using Netorrent.P2P.Messages;
using Netorrent.Statistics;
using R3;
using Shouldly;

namespace Netorrent.Tests.P2P;

[Timeout(10_000)]
public class RequestSchedulerTests
{
    [Test]
    public async Task Should_Download_Block_From_Peer(CancellationToken cancellationToken)
    {
        var logger = NullLogger.Instance;
        var seederBitfield = new Bitfield(1, true);
        var bitfield = new Bitfield(1, false);
        await using var seederPeerConnection = new FakePeerConnection(bitfield, seederBitfield);
        seederPeerConnection.PeerChoking.Value = false;
        seederPeerConnection.AmInterested.Value = true;
        await using var requestScheduler = new RequestScheduler(
            new Dictionary<PeerEndpoint, IPeerConnection>()
            {
                [seederPeerConnection.PeerEndpoint] = seederPeerConnection,
            },
            new PiecePicker(bitfield, 16 * 1024, 16 * 1024 * 4, 16 * 1024 * 4), //whole file is 4 blocks and 1 piece
            bitfield,
            new DataStatistics(4),
            TimeSpan.Zero,
            new FakePieceStorage(),
            logger
        );
        var completion = bitfield.StateChanged.FirstAsync(cancellationToken);
        var requestTask = requestScheduler.StartAsync(cancellationToken);

        using var sentDipose = seederPeerConnection.SentRequests.SubscribeAwait(
            async (i, ct) =>
            {
                var array = ArrayPool<byte>.Shared.Rent(i.Length);
                var rentedArray = new RentedArray<byte>(array, i.Length);
                await requestScheduler.ReceiveBlockAsync(
                    new Block(i.Index, i.Begin, rentedArray, seederPeerConnection),
                    ct
                );
            }
        );
        requestScheduler.TryRequest(seederPeerConnection);

        await completion.ShouldNotThrowAsync();
        bitfield.IsComplete.ShouldBeTrue();
    }
}
