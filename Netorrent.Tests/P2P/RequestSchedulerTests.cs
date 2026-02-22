using System.Buffers;
using Microsoft.Extensions.Logging;
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

    static ILogger Logger => NullLogger.Instance;

    static Bitfield CreateBitfield(bool full) => new(PiecesCount, full);

    static async Task<FakePeerConnection> CreatePeerConnectionAsync(
        Bitfield myBitfield,
        Bitfield theirBitfield
    )
    {
        var peer = new FakePeerConnection(myBitfield, theirBitfield, BlockSize);
        peer.PeerChoking.Value = false;
        peer.AmInterested.Value = true;
        return peer;
    }

    static RequestScheduler CreateScheduler(
        IReadOnlyDictionary<PeerEndpoint, IPeerConnection> peers,
        Bitfield bitfield,
        TimeSpan timeout
    )
    {
        return new RequestScheduler(
            peers,
            new PiecePicker(bitfield, BlockSize, PieceSize, TotalSize),
            bitfield,
            new DataStatistics(peers.Count),
            TimeSpan.Zero,
            timeout,
            new FakePieceStorage(),
            Logger
        );
    }

    static IDisposable RespondWithBlocks(
        RequestScheduler scheduler,
        FakePeerConnection peer,
        int blockCount
    )
    {
        return peer
            .SentRequests.Take(blockCount)
            .SubscribeAwait(
                async (request, ct) =>
                {
                    var rentedArray = new RentedArray<byte>(request.Length);
                    try
                    {
                        await scheduler.ReceiveBlockAsync(
                            new Block(request.Index, request.Begin, rentedArray, peer),
                            ct
                        );
                    }
                    catch
                    {
                        rentedArray.Dispose();
                        throw;
                    }
                }
            );
    }

    [Test]
    public async Task Should_Download_Piece_From_Peer(CancellationToken cancellationToken)
    {
        var seederBitfield = CreateBitfield(true);
        var bitfield = CreateBitfield(false);

        await using var peer1 = await CreatePeerConnectionAsync(bitfield, seederBitfield);
        await using var peer2 = await CreatePeerConnectionAsync(bitfield, seederBitfield);

        var peers = new Dictionary<PeerEndpoint, IPeerConnection>
        {
            [peer1.PeerEndpoint] = peer1,
            [peer2.PeerEndpoint] = peer2,
        };

        await using var scheduler = CreateScheduler(peers, bitfield, 30.Seconds);

        var cts = cancellationToken.WithTimeout(3.Seconds);
        var peer2RequestTask = peer2.SentRequests.FirstAsync(cts.Token);
        var completion = bitfield.StateChanged.FirstAsync(cancellationToken);
        _ = scheduler.StartAsync(cancellationToken);

        using var _response = RespondWithBlocks(scheduler, peer1, 4);
        scheduler.TryRequest(peer1);

        await completion.ShouldNotThrowAsync();
        (await completion).ShouldBe(0);
        await peer2RequestTask.ShouldNotThrowAsync();
        peer2RequestTask.Status.ShouldBe(TaskStatus.Canceled);
    }

    [Test]
    public async Task Should_Not_Download_Piece_From_Peer(CancellationToken cancellationToken)
    {
        var seederBitfield = CreateBitfield(true);
        var bitfield = CreateBitfield(false);

        await using var peer = new FakePeerConnection(bitfield, seederBitfield, BlockSize);
        peer.PeerChoking.Value = true;
        peer.AmInterested.Value = true;

        var peers = new Dictionary<PeerEndpoint, IPeerConnection> { [peer.PeerEndpoint] = peer };
        await using var scheduler = CreateScheduler(peers, bitfield, 30.Seconds);

        _ = scheduler.StartAsync(cancellationToken);
        using var cts = cancellationToken.WithTimeout(3.Seconds);
        var countTask = peer.SentRequests.CountAsync(cts.Token);
        scheduler.TryRequest(peer);

        await countTask.ShouldNotThrowAsync();
        countTask.Status.ShouldBe(TaskStatus.Canceled);
        bitfield.IsComplete.ShouldBeFalse();
    }

    [Test]
    public async Task Should_Timeout_Blocks_From_Peer(CancellationToken cancellationToken)
    {
        var seederBitfield = CreateBitfield(true);
        var bitfield = CreateBitfield(false);

        await using var peer1 = await CreatePeerConnectionAsync(bitfield, seederBitfield);
        await using var peer2 = await CreatePeerConnectionAsync(bitfield, seederBitfield);

        var peers = new Dictionary<PeerEndpoint, IPeerConnection>
        {
            [peer1.PeerEndpoint] = peer1,
            [peer2.PeerEndpoint] = peer2,
        };

        await using var scheduler = CreateScheduler(peers, bitfield, 1.Seconds);

        _ = scheduler.StartAsync(cancellationToken);
        var countTask = Task.WhenAll(
            peer1.SentRequests.Take(4).CountAsync(cancellationToken),
            peer2.SentRequests.Take(4).CountAsync(cancellationToken)
        );
        scheduler.TryRequest(peer1);

        await countTask.ShouldNotThrowAsync();
        bitfield.IsComplete.ShouldBeFalse();
    }

    [Test]
    public async Task Should_Request_All_Blocks_From_All_Peers_On_Endgame(
        CancellationToken cancellationToken
    )
    {
        var seederBitfield = CreateBitfield(true);
        var bitfield = CreateBitfield(false);

        await using var peer1 = await CreatePeerConnectionAsync(bitfield, seederBitfield);
        await using var peer2 = await CreatePeerConnectionAsync(bitfield, seederBitfield);
        await using var peer3 = await CreatePeerConnectionAsync(bitfield, seederBitfield);

        var peers = new Dictionary<PeerEndpoint, IPeerConnection>
        {
            [peer1.PeerEndpoint] = peer1,
            [peer2.PeerEndpoint] = peer2,
            [peer3.PeerEndpoint] = peer3,
        };

        await using var scheduler = CreateScheduler(peers, bitfield, 30.Seconds);
        var blocksPerPiece = PieceSize / BlockSize;

        using var response = RespondWithBlocks(
            scheduler,
            peer1,
            blocksPerPiece * TotalSize / PieceSize
        );
        _ = scheduler.StartAsync(cancellationToken);

        var countTask = Task.WhenAll(
            peer1.SentRequests.Take(blocksPerPiece).CountAsync(cancellationToken),
            peer2.SentRequests.Take(blocksPerPiece).CountAsync(cancellationToken),
            peer3.SentRequests.Take(blocksPerPiece).CountAsync(cancellationToken)
        );
        scheduler.TryRequest(peer1);

        await countTask.ShouldNotThrowAsync();
        countTask.Status.ShouldBe(TaskStatus.RanToCompletion);
        bitfield.IsComplete.ShouldBeTrue();
    }
}
