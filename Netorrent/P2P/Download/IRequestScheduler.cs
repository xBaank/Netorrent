using Netorrent.P2P.Messages;

namespace Netorrent.P2P.Download;

internal interface IRequestScheduler : IAsyncDisposable
{
    Task StartAsync(CancellationToken cancellationToken);
    void DecreaseRarity(int index);
    void IncreaseRarity(int index);
    ValueTask RequestSlotAsync(PeerConnection peerConnection, CancellationToken cancellationToken);
    ValueTask FreeSlotAsync(PeerConnection peerConnection, CancellationToken cancellationToken);
    ValueTask ReceiveBlockAsync(Block block, CancellationToken cancellationToken);
}
