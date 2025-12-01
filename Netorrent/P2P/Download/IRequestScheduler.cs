using Netorrent.P2P.Messages;

namespace Netorrent.P2P.Download;

internal interface IRequestScheduler : IAsyncDisposable
{
    void DecreaseRarity(int index);
    void IncreaseRarity(int index);
    ValueTask OnPeerUnchockedAsync(PeerConnection peer, CancellationToken cancellationToken);
    ValueTask ReceiveBlockAsync(Block block, CancellationToken cancellationToken);
}
