using System.Net;
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
    public SynchronizedReactiveProperty<bool> ActiveDownloader { get; } = new(false);

    public PeerEndpoint PeerEndpoint { get; } = new(new(IPAddress.Loopback, 50), new());
    public TimeSpan ConnectionDuration => throw new NotImplementedException();

    public SpeedTracker DownloadTracker => new();

    public SpeedTracker UploadTracker => new();

    public ulong RequestedBlocksCount => throw new NotImplementedException();

    public ulong UploadRequestedBlocksCount => _uploadRequestedCount;

    public Bitfield MyBitField => myBitfield;

    public Bitfield? PeerBitField => throw new NotImplementedException();

    public PeerRequestWindow PeerRequestWindow => throw new NotImplementedException();

    ReadOnlyReactiveProperty<bool> IPeerConnection.AmChoking => AmChoking;

    ReadOnlyReactiveProperty<bool> IPeerConnection.AmInterested => AmInterested;

    ReadOnlyReactiveProperty<bool> IPeerConnection.PeerChoking => PeerChoking;

    ReadOnlyReactiveProperty<bool> IPeerConnection.PeerInterested => PeerInterested;
    ReactiveProperty<bool> IPeerConnection.ActiveDownloader => ActiveDownloader;

    public TimeSpan TimeSinceReceivedBlock => 0.Seconds;

    public TimeSpan TimeSinceSentBlock => 0.Seconds;

    private ulong _uploadRequestedCount = 0;

    public ulong DecrementRequestedBlock()
    {
        throw new NotImplementedException();
    }

    public ulong IncrementUploadRequested() => _uploadRequestedCount++;

    public ulong DecrementUploadRequested() => _uploadRequestedCount--;

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public ulong IncrementRequestedBlock()
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

    public void Unchoke()
    {
        AmChoking.Value = false;
    }

    public void Choke()
    {
        AmChoking.Value = true;
    }
}
