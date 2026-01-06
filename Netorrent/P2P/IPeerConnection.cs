using Netorrent.IO;
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
    ReactiveProperty<bool> ActiveDownloader { get; }
    TimeSpan ConnectionDuration { get; }
    SpeedTracker DownloadTracker { get; }
    SpeedTracker UploadTracker { get; }
    Bitfield MyBitField { get; }
    Bitfield? PeerBitField { get; }
    PeerEndpoint PeerEndpoint { get; }
    PeerRequestWindow PeerRequestWindow { get; }
    ulong RequestedBlocksCount { get; }
    ulong UploadRequestedBlocksCount { get; }
    TimeSpan TimeSinceReceivedBlock { get; }
    TimeSpan TimeSinceSentBlock { get; }

    ulong DecrementRequestedBlock();
    ulong DecrementUploadRequested();
    ulong IncrementRequestedBlock();
    ulong IncrementUploadRequested();
    bool TrySendBlock(Block block);
    bool TrySendCancel(RequestBlock request);
    bool TrySendRequest(RequestBlock nextBlock);
    void Unchoke();
    void Choke();
    Task StartAsync(CancellationToken cancellationToken);
}
