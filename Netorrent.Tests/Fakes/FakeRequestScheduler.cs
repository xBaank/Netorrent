using Netorrent.P2P;
using Netorrent.P2P.Download;
using Netorrent.P2P.Messages;

namespace Netorrent.Tests.Fakes;

internal class FakeRequestScheduler : IRequestScheduler
{
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public ValueTask ReceiveBlockAsync(Block block, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public Task StartAsync(CancellationToken cancellationToken) =>
        Task.Delay(-1, cancellationToken);

    public void TryRequest(IPeerConnection peerConnection) { }
}
