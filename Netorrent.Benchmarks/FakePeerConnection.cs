using System.Net;
using Netorrent.Extensions;
using Netorrent.P2P;
using Netorrent.P2P.Download;
using Netorrent.P2P.Measurement;
using Netorrent.P2P.Messages;
using R3;

namespace Netorrent.Benchmarks;

internal class FakePeerConnection(Bitfield myBitfield, Bitfield peerBitifield, int blockSize)
    : IPeerConnection
{
    public Subject<Block> SentBlocks = new();
    public Subject<RequestBlock> SentRequests = new();
    public SynchronizedReactiveProperty<bool> AmChoking { get; } = new(true);

    public SynchronizedReactiveProperty<bool> AmInterested { get; } = new(false);

    public SynchronizedReactiveProperty<bool> PeerChoking { get; } = new(true);

    public SynchronizedReactiveProperty<bool> PeerInterested { get; } = new(false);
    public SynchronizedReactiveProperty<bool> ActiveDownloader { get; } = new(false);

    public PeerEndpoint PeerEndpoint { get; } = new(new(IPAddress.Loopback, 50), new());
    public TimeSpan ConnectionDuration => throw new NotImplementedException();

    public SpeedTracker DownloadTracker => new();

    public SpeedTracker UploadTracker => new();

    public ulong RequestedBlocksCount => _requestedBlocksCount;

    private ulong _requestedBlocksCount = 0;

    public ulong UploadRequestedBlocksCount => _uploadRequestedCount;

    public Bitfield MyBitField => myBitfield;

    public Bitfield? PeerBitField => peerBitifield;

    public PeerRequestWindow PeerRequestWindow => _peerRequestWindow;

    private readonly PeerRequestWindow _peerRequestWindow = new(blockSize);

    ReadOnlyReactiveProperty<bool> IPeerConnection.AmChoking => AmChoking;

    ReadOnlyReactiveProperty<bool> IPeerConnection.AmInterested => AmInterested;

    ReadOnlyReactiveProperty<bool> IPeerConnection.PeerChoking => PeerChoking;

    ReadOnlyReactiveProperty<bool> IPeerConnection.PeerInterested => PeerInterested;
    ReactiveProperty<bool> IPeerConnection.ActiveDownloader => ActiveDownloader;

    public TimeSpan TimeSinceReceivedBlock => 0.Seconds;

    public TimeSpan TimeSinceSentBlock => 0.Seconds;

    private ulong _uploadRequestedCount = 0;

    public ulong DecrementRequestedBlock() => _requestedBlocksCount--;

    public ulong IncrementUploadRequested() => _uploadRequestedCount++;

    public ulong DecrementUploadRequested() => _uploadRequestedCount--;

    public async ValueTask DisposeAsync()
    {
        SentBlocks.OnCompleted();
        SentRequests.OnCompleted();
    }

    public ulong IncrementRequestedBlock() => _requestedBlocksCount++;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public bool TrySendBlock(Block block)
    {
        SentBlocks.OnNext(block);
        return true;
    }

    public bool TrySendCancel(RequestBlock request)
    {
        return true;
    }

    public bool TrySendRequest(RequestBlock nextBlock)
    {
        SentRequests.OnNext(nextBlock);
        return true;
    }

    public void Unchoke()
    {
        AmChoking.Value = false;
    }

    public void Choke()
    {
        AmChoking.Value = true;
    }
}
