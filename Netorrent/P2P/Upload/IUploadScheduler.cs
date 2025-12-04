using Netorrent.P2P.Messages;

namespace Netorrent.P2P.Upload;

internal interface IUploadScheduler : IAsyncDisposable
{
    ValueTask RequestSlotAsync(PeerConnection peerConnection, CancellationToken cancellationToken);
    ValueTask<bool> AddRequestAsync(RequestBlock request, CancellationToken cancellationToken);
    void CancelRequest(RequestBlock request);
    ValueTask FreeSlotAsync(PeerConnection peerConnection, CancellationToken cancellationToken);
}
