using Netorrent.P2P.Messages;

namespace Netorrent.P2P.Upload;

internal interface IUploadScheduler : IAsyncDisposable
{
    bool AddChokedSlot();
    ValueTask<bool> AddRequestAsync(RequestBlock request, CancellationToken cancellationToken);
    void CancelRequest(RequestBlock request);
    bool RemoveChokedSlot();
}
