using Netorrent.P2P.Messages;

namespace Netorrent.P2P.Upload;

internal interface IUploadScheduler : IAsyncDisposable
{
    ValueTask<bool> AddRequestAsync(RequestBlock request, CancellationToken cancellationToken);
    void CancelRequest(RequestBlock request);
    ValueTask RequestSlotAsync(PeerConnection peerConnection, CancellationToken cancellationToken);
    ValueTask FreeSlotAsync(PeerConnection peerConnection, CancellationToken cancellationToken);
}
