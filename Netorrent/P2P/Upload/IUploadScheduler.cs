using Netorrent.P2P.Messages;

namespace Netorrent.P2P.Upload;

internal interface IUploadScheduler : IAsyncDisposable
{
    Task StartAsync(CancellationToken cancellationToken);
    ValueTask AddRequestAsync(RequestBlock request, CancellationToken cancellationToken);
    void CancelRequest(RequestBlock request);
    ValueTask AddPeerAsync(IPeerConnection peerConnection, CancellationToken cancellationToken);
    ValueTask RemovePeerAsync(IPeerConnection peerConnection, CancellationToken cancellationToken);
}
