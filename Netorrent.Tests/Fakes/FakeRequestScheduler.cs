using Netorrent.P2P;
using Netorrent.P2P.Download;
using Netorrent.P2P.Messages;

namespace Netorrent.Tests.Fakes;

internal class FakeRequestScheduler : IRequestScheduler
{
    public void DecreaseRarity(int index)
    {
        throw new NotImplementedException();
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public ValueTask FreeSlotAsync(
        PeerConnection peerConnection,
        CancellationToken cancellationToken
    ) => ValueTask.CompletedTask;

    public void IncreaseRarity(int index)
    {
        throw new NotImplementedException();
    }

    public ValueTask ReceiveBlockAsync(Block block, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public ValueTask RequestSlotAsync(
        PeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        throw new NotImplementedException();
    }

    public Task StartAsync(CancellationToken cancellationToken) =>
        Task.Delay(-1, cancellationToken);
}
