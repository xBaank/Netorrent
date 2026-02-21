using Netorrent.P2P;
using Netorrent.P2P.Messages;
using Netorrent.P2P.Upload;

namespace Netorrent.Tests.Fakes;

internal class FakeUploadScheduler : IUploadScheduler
{
    public async ValueTask AddPeerAsync(
        IPeerConnection peerConnection,
        CancellationToken cancellationToken
    ) { }

    public ValueTask AddRequestAsync(RequestBlock request, CancellationToken cancellationToken) =>
        ValueTask.CompletedTask;

    public void CancelRequest(RequestBlock request) { }

    public void TryRunRound(IPeerConnection peerConnection) { }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public ValueTask RemovePeerAsync(
        IPeerConnection peerConnection,
        CancellationToken cancellationToken
    ) => ValueTask.CompletedTask;

    public Task StartAsync(CancellationToken cancellationToken) =>
        Task.Delay(-1, cancellationToken);
}
