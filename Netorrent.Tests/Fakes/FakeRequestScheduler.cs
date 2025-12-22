using Netorrent.P2P;
using Netorrent.P2P.Download;
using Netorrent.P2P.Messages;

namespace Netorrent.Tests.Fakes;

internal class FakeRequestScheduler : IRequestScheduler
{
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public ValueTask FreeSlotAsync(
        IPeerConnection peerConnection,
        CancellationToken cancellationToken
    ) => ValueTask.CompletedTask;

    public ValueTask ReceiveBlockAsync(Block block, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public ValueTask RequestSlotAsync(
        IPeerConnection peerConnection,
        CancellationToken cancellationToken
    ) => ValueTask.CompletedTask;

    public Task StartAsync(CancellationToken cancellationToken) =>
        Task.Delay(-1, cancellationToken);
}
