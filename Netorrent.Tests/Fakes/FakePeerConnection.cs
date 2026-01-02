using Netorrent.P2P;
using Netorrent.P2P.Download;
using Netorrent.P2P.Measurement;
using Netorrent.P2P.Messages;
using R3;

internal class FakePeerConnection : IPeerConnection
{
    public Subject<Block> SentBlocks = new();
    public ReactiveProperty<bool> AmChoking => new(true);

    public ReactiveProperty<bool> AmInterested => new(false);

    public ReactiveProperty<bool> PeerChoking => new(true);

    public ReactiveProperty<bool> PeerInterested => new(false);

    public TimeSpan ConnectionDuration => throw new NotImplementedException();

    public SpeedTracker DownloadSpeedTracker => throw new NotImplementedException();

    public SpeedTracker UploadSpeedTracker => throw new NotImplementedException();

    public PeerEndpoint PeerEndpoint => throw new NotImplementedException();

    public int RequestedBlocksCount => throw new NotImplementedException();

    public int UploadRequestedBlocksCount => _uploadRequestedCount;

    public Bitfield MyBitField => throw new NotImplementedException();

    public Bitfield? PeerBitField => throw new NotImplementedException();

    public PeerRequestWindow PeerRequestWindow => throw new NotImplementedException();

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

    public bool TrySendUnchoked()
    {
        AmChoking.Value = false;
        return true;
    }

    public bool TrySendChoked()
    {
        AmChoking.Value = true;
        return true;
    }
}
