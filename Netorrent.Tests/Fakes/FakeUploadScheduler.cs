using Netorrent.P2P;
using Netorrent.P2P.Messages;
using Netorrent.P2P.Upload;

namespace Netorrent.Tests.Fakes;

internal class FakeUploadScheduler : IUploadScheduler
{
    public ValueTask AddRequestAsync(RequestBlock request, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public void CancelRequest(RequestBlock request)
    {
        throw new NotImplementedException();
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public ValueTask FreeSlotAsync(
        PeerConnection peerConnection,
        CancellationToken cancellationToken
    ) => ValueTask.CompletedTask;

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
