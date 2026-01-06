using Netorrent.P2P.Messages;

namespace Netorrent.P2P.Download;

internal interface IRequestScheduler : IAsyncDisposable
{
    Task StartAsync(CancellationToken cancellationToken);
    ValueTask CheckSlotAsync(IPeerConnection peerConnection, CancellationToken cancellationToken);
    ValueTask ReceiveBlockAsync(Block block, CancellationToken cancellationToken);
}
