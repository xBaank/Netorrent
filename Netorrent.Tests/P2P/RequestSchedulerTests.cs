using System.Buffers;
using Microsoft.Extensions.Logging.Abstractions;
using Netorrent.Extensions;
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
    const int BlockSize = 1024 * 16;
    const int PieceSize = BlockSize * 4;
    const int TotalSize = PieceSize * 4;
    const int PiecesCount = TotalSize / PieceSize;

    [Test]
    public async Task Should_Download_Piece_From_Peer(CancellationToken cancellationToken)
    {
        var logger = NullLogger.Instance;
        var seederBitfield = new Bitfield(PiecesCount, true);
        var bitfield = new Bitfield(PiecesCount, false);
        await using var seederPeerConnection = new FakePeerConnection(
            bitfield,
            seederBitfield,
            BlockSize
        );
        await using var seederPeerConnection2 = new FakePeerConnection(
            bitfield,
            seederBitfield,
            BlockSize
        );
        seederPeerConnection.PeerChoking.Value = false;
        seederPeerConnection.AmInterested.Value = true;
        seederPeerConnection2.PeerChoking.Value = false;
        seederPeerConnection2.AmInterested.Value = true;
        await using var requestScheduler = new RequestScheduler(
            new Dictionary<PeerEndpoint, IPeerConnection>()
            {
                [seederPeerConnection.PeerEndpoint] = seederPeerConnection,
                [seederPeerConnection2.PeerEndpoint] = seederPeerConnection2,
            },
            new PiecePicker(bitfield, BlockSize, PieceSize, TotalSize), //whole file is 4 blocks and 1 piece
            bitfield,
            new DataStatistics(4),
            TimeSpan.Zero,
            30.Seconds,
            new FakePieceStorage(),
            logger
        );

        var cts = cancellationToken.WithTimeout(3.Seconds);
        var seeder2SentRequestTask = seederPeerConnection2.SentRequests.FirstAsync(cts.Token);
        var completion = bitfield.StateChanged.FirstAsync(cancellationToken);
        var requestTask = requestScheduler.StartAsync(cancellationToken);

        using var sentDipose = seederPeerConnection
            .SentRequests.Take(4)
            .SubscribeAwait(
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
        (await completion).ShouldBe(0);
        await seeder2SentRequestTask.ShouldNotThrowAsync();
        seeder2SentRequestTask.Status.ShouldBe(TaskStatus.Canceled);
    }

    [Test]
    public async Task Should_Not_Download_Piece_From_Peer(CancellationToken cancellationToken)
    {
        var logger = NullLogger.Instance;
        var seederBitfield = new Bitfield(PiecesCount, true);
        var bitfield = new Bitfield(PiecesCount, false);
        await using var seederPeerConnection = new FakePeerConnection(
            bitfield,
            seederBitfield,
            BlockSize
        );
        seederPeerConnection.PeerChoking.Value = true;
        seederPeerConnection.AmInterested.Value = true;
        await using var requestScheduler = new RequestScheduler(
            new Dictionary<PeerEndpoint, IPeerConnection>()
            {
                [seederPeerConnection.PeerEndpoint] = seederPeerConnection,
            },
            new PiecePicker(bitfield, BlockSize, PieceSize, TotalSize),
            bitfield,
            new DataStatistics(4),
            TimeSpan.Zero,
            30.Seconds,
            new FakePieceStorage(),
            logger
        );

        var requestTask = requestScheduler.StartAsync(cancellationToken);
        using var cts = cancellationToken.WithTimeout(3.Seconds);
        var countTask = seederPeerConnection.SentRequests.CountAsync(cts.Token);
        requestScheduler.TryRequest(seederPeerConnection);

        await countTask.ShouldNotThrowAsync();
        countTask.Status.ShouldBe(TaskStatus.Canceled);
        bitfield.IsComplete.ShouldBeFalse();
    }

    [Test]
    public async Task Should_Timeout_Blocks_From_Peer(CancellationToken cancellationToken)
    {
        var logger = NullLogger.Instance;
        var seederBitfield = new Bitfield(PiecesCount, true);
        var bitfield = new Bitfield(PiecesCount, false);
        await using var seederPeerConnection = new FakePeerConnection(
            bitfield,
            seederBitfield,
            BlockSize
        );
        await using var seederPeerConnection2 = new FakePeerConnection(
            bitfield,
            seederBitfield,
            BlockSize
        );
        seederPeerConnection.PeerChoking.Value = false;
        seederPeerConnection.AmInterested.Value = true;
        seederPeerConnection2.PeerChoking.Value = false;
        seederPeerConnection2.AmInterested.Value = true;
        await using var requestScheduler = new RequestScheduler(
            new Dictionary<PeerEndpoint, IPeerConnection>()
            {
                [seederPeerConnection.PeerEndpoint] = seederPeerConnection,
                [seederPeerConnection2.PeerEndpoint] = seederPeerConnection2,
            },
            new PiecePicker(bitfield, BlockSize, PieceSize, TotalSize),
            bitfield,
            new DataStatistics(4),
            TimeSpan.Zero,
            1.Seconds,
            new FakePieceStorage(),
            logger
        );

        var requestTask = requestScheduler.StartAsync(cancellationToken);
        var countTask = Task.WhenAll(
            seederPeerConnection.SentRequests.Take(4).CountAsync(cancellationToken),
            seederPeerConnection2.SentRequests.Take(4).CountAsync(cancellationToken)
        );
        requestScheduler.TryRequest(seederPeerConnection);

        await countTask.ShouldNotThrowAsync();
        bitfield.IsComplete.ShouldBeFalse();
    }

    [Test]
    public async Task Should_Request_All_Blocks_From_All_Peers_On_Endgame(
        CancellationToken cancellationToken
    )
    {
        var logger = NullLogger.Instance;
        var seederBitfield = new Bitfield(PiecesCount, true);
        var bitfield = new Bitfield(PiecesCount, false);
        await using var seederPeerConnection = new FakePeerConnection(
            bitfield,
            seederBitfield,
            BlockSize
        );
        await using var seederPeerConnection2 = new FakePeerConnection(
            bitfield,
            seederBitfield,
            BlockSize
        );
        await using var seederPeerConnection3 = new FakePeerConnection(
            bitfield,
            seederBitfield,
            BlockSize
        );
        seederPeerConnection.PeerChoking.Value = false;
        seederPeerConnection.AmInterested.Value = true;
        seederPeerConnection2.PeerChoking.Value = false;
        seederPeerConnection2.AmInterested.Value = true;
        seederPeerConnection3.PeerChoking.Value = false;
        seederPeerConnection3.AmInterested.Value = true;
        await using var requestScheduler = new RequestScheduler(
            new Dictionary<PeerEndpoint, IPeerConnection>()
            {
                [seederPeerConnection.PeerEndpoint] = seederPeerConnection,
                [seederPeerConnection2.PeerEndpoint] = seederPeerConnection2,
                [seederPeerConnection3.PeerEndpoint] = seederPeerConnection3,
            },
            new PiecePicker(bitfield, BlockSize, PieceSize, TotalSize),
            bitfield,
            new DataStatistics(4),
            TimeSpan.Zero,
            30.Seconds,
            new FakePieceStorage(),
            logger
        );

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

        var requestTask = requestScheduler.StartAsync(cancellationToken);
        var countTask = Task.WhenAll(
            seederPeerConnection
                .SentRequests.Take(PieceSize / BlockSize)
                .CountAsync(cancellationToken),
            seederPeerConnection2
                .SentRequests.Take(PieceSize / BlockSize)
                .CountAsync(cancellationToken),
            seederPeerConnection3
                .SentRequests.Take(PieceSize / BlockSize)
                .CountAsync(cancellationToken)
        );
        requestScheduler.TryRequest(seederPeerConnection);

        await countTask.ShouldNotThrowAsync();
        countTask.Status.ShouldBe(TaskStatus.RanToCompletion);
        bitfield.IsComplete.ShouldBeTrue();
    }
}
