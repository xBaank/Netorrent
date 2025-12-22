using Netorrent.P2P.Download;
using Netorrent.P2P.Measurement;
using Netorrent.P2P.Messages;
using R3;

namespace Netorrent.P2P;

internal interface IPeerConnection : IAsyncDisposable
{
    ReactiveProperty<bool> AmChoking { get; }
    ReactiveProperty<bool> AmInterested { get; }
    TimeSpan ConnectionDuration { get; }
    SpeedTracker DownloadSpeedTracker { get; }
    Bitfield MyBitField { get; }
    Bitfield? PeerBitField { get; }
    ReactiveProperty<bool> PeerChoking { get; }
    PeerEndpoint PeerEndpoint { get; }
    ReactiveProperty<bool> PeerInterested { get; }
    PeerRequestWindow PeerRequestWindow { get; }
    int RequestedBlocksCount { get; }
    int UploadRequestedBlocksCount { get; }
    SpeedTracker UploadSpeedTracker { get; }

    int DecrementRequestedBlock();
    int DecrementUploadRequested();
    int IncrementRequestedBlock();
    int IncrementUploadRequested();
    ValueTask SendBlockAsync(Block block, CancellationToken cancellationToken);
    ValueTask SendCancelAsync(RequestBlock request, CancellationToken cancellationToken);
    ValueTask SendRequestAsync(RequestBlock nextBlock, CancellationToken cancellationToken);
    Task SendUnchokedAsync(CancellationToken cancellationToken);
    Task StartAsync(CancellationToken cancellationToken);
}
