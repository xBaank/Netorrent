using Netorrent.P2P.Messages;

namespace Netorrent.P2P.Download;

internal interface IRequestScheduler : IAsyncDisposable
{
    Task StartAsync(CancellationToken cancellationToken);
    void TryRequest(IPeerConnection peerConnection);
    ValueTask ReceiveBlockAsync(Block block, CancellationToken cancellationToken);
}
