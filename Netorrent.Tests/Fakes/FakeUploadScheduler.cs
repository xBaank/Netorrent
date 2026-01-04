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

    public ValueTask AddRequestAsync(RequestBlock request, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public void CancelRequest(RequestBlock request)
    {
        throw new NotImplementedException();
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public async ValueTask RemovePeerAsync(
        IPeerConnection peerConnection,
        CancellationToken cancellationToken
    ) { }

    public Task StartAsync(CancellationToken cancellationToken) =>
        Task.Delay(-1, cancellationToken);
}
