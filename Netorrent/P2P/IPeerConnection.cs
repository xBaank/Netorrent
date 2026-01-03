using Netorrent.P2P.Download;
using Netorrent.P2P.Measurement;
using Netorrent.P2P.Messages;
using R3;

namespace Netorrent.P2P;

internal interface IPeerConnection : IAsyncDisposable
{
    ReadOnlyReactiveProperty<bool> AmChoking { get; }
    ReadOnlyReactiveProperty<bool> AmInterested { get; }
    ReadOnlyReactiveProperty<bool> PeerChoking { get; }
    ReadOnlyReactiveProperty<bool> PeerInterested { get; }
    TimeSpan ConnectionDuration { get; }
    SpeedTracker DownloadTracker { get; }
    SpeedTracker UploadTracker { get; }
    Bitfield MyBitField { get; }
    Bitfield? PeerBitField { get; }
    PeerEndpoint PeerEndpoint { get; }
    PeerRequestWindow PeerRequestWindow { get; }
    int RequestedBlocksCount { get; }
    int UploadRequestedBlocksCount { get; }

    int DecrementRequestedBlock();
    int DecrementUploadRequested();
    int IncrementRequestedBlock();
    int IncrementUploadRequested();
    bool TrySendBlock(Block block);
    bool TrySendCancel(RequestBlock request);
    bool TrySendRequest(RequestBlock nextBlock);
    bool TrySendUnchoked();
    bool TrySendChoked();
    Task StartAsync(CancellationToken cancellationToken);
}
