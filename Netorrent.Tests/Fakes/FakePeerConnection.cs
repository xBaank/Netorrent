using Netorrent.Extensions;
using Netorrent.P2P;
using Netorrent.P2P.Download;
using Netorrent.P2P.Measurement;
using Netorrent.P2P.Messages;
using R3;

internal class FakePeerConnection(Bitfield myBitfield) : IPeerConnection
{
    public Subject<Block> SentBlocks = new();
    public SynchronizedReactiveProperty<bool> AmChoking { get; } = new(true);

    public SynchronizedReactiveProperty<bool> AmInterested { get; } = new(false);

    public SynchronizedReactiveProperty<bool> PeerChoking { get; } = new(true);

    public SynchronizedReactiveProperty<bool> PeerInterested { get; } = new(false);

    public TimeSpan ConnectionDuration => throw new NotImplementedException();

    public SpeedTracker DownloadTracker => new();

    public SpeedTracker UploadTracker => new();

    public PeerEndpoint PeerEndpoint => throw new NotImplementedException();

    public int RequestedBlocksCount => throw new NotImplementedException();

    public int UploadRequestedBlocksCount => _uploadRequestedCount;

    public Bitfield MyBitField => myBitfield;

    public Bitfield? PeerBitField => throw new NotImplementedException();

    public PeerRequestWindow PeerRequestWindow => throw new NotImplementedException();

    ReadOnlyReactiveProperty<bool> IPeerConnection.AmChoking => AmChoking;

    ReadOnlyReactiveProperty<bool> IPeerConnection.AmInterested => AmInterested;

    ReadOnlyReactiveProperty<bool> IPeerConnection.PeerChoking => PeerChoking;

    ReadOnlyReactiveProperty<bool> IPeerConnection.PeerInterested => PeerInterested;

    public TimeSpan TimeSinceReceivedBlock => 0.Seconds;

    public TimeSpan TimeSinceSentBlock => 0.Seconds;

    private int _uploadRequestedCount = 0;

    public int DecrementRequestedBlock()
    {
        throw new NotImplementedException();
    }

    public int IncrementUploadRequested() => _uploadRequestedCount++;

    public int DecrementUploadRequested() => _uploadRequestedCount--;

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public int IncrementRequestedBlock()
    {
        throw new NotImplementedException();
    }

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
        throw new NotImplementedException();
    }

    public bool TrySendRequest(RequestBlock nextBlock)
    {
        throw new NotImplementedException();
    }

    public async ValueTask UnchokeAsync(CancellationToken cancellationToken)
    {
        AmChoking.Value = false;
    }

    public async ValueTask ChokeAsync(CancellationToken cancellationToken)
    {
        AmChoking.Value = true;
    }
}
