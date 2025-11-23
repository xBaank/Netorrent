using System.Collections.Concurrent;
using System.Net;
using System.Threading.Channels;
using Netorrent.IO;
using Netorrent.P2P.Messages;

namespace Netorrent.P2P.Upload;

internal class UploadScheduler(FileManager fileManager) : IAsyncDisposable
{
    private readonly Channel<RequestBlock> _pendingRequests = Channel.CreateBounded<RequestBlock>(
        new BoundedChannelOptions(128) { SingleWriter = false, SingleReader = true }
    );
    private readonly ConcurrentDictionary<
        (int Index, int Begin, int Length),
        RequestBlock
    > _requestByIBL = [];

    private readonly Lock _unchokedSlotsLock = new();
    private int _unchokedSlots = 0;
    private Task? _waitTask;

    public Task? WaitTask => _waitTask;

    public void Start(CancellationToken cancellationToken) =>
        _waitTask ??= ProcessRequestsAsync(cancellationToken);

    private async Task ProcessRequestsAsync(CancellationToken cancellationToken)
    {
        await foreach (var requestBlock in _pendingRequests.Reader.ReadAllAsync(cancellationToken))
        {
            if (requestBlock is { State: RequestBlockState.Cancelled } or { RequestedFrom: null })
                continue;

            var pieceData = await fileManager.ReadPieceAsync(
                requestBlock.Index,
                requestBlock.Begin,
                requestBlock.Length,
                cancellationToken
            );

            using var block = new Block(requestBlock.Index, requestBlock.Begin, pieceData);
            await requestBlock.RequestedFrom.SendBlockAsync(block, cancellationToken);
        }
    }

    public void AddChokedSlot(PeerConnection peerConnection)
    {
        lock (_unchokedSlotsLock)
        {
            if (peerConnection.AmChocking)
                return;
            if (_unchokedSlots >= 4)
                return;

            _unchokedSlots++;
        }
    }

    public void RemoveChokedSlot(PeerConnection peerConnection)
    {
        lock (_unchokedSlotsLock)
        {
            if (!peerConnection.AmChocking)
                return;
            if (_unchokedSlots <= 0)
                return;
            _unchokedSlots--;
        }
    }

    public async ValueTask AddRequestAsync(
        RequestBlock request,
        CancellationToken cancellationToken
    )
    {
        var desired = request.RequestedFrom!.UploadSpeedTracker.CurrentBps.Kbps / 50;
        var max = Math.Clamp(desired, min: 2, max: 8);

        if (request.RequestedFrom!.UploadRequestedBlocksCount >= max)
            return;

        await _pendingRequests.Writer.WriteAsync(request, cancellationToken);
        var key = (request.Index, request.Begin, request.Length);
        _requestByIBL.TryAdd(key, request);
    }

    public void CancelRequest(RequestBlock request)
    {
        var key = (request.Index, request.Begin, request.Length);
        if (!_requestByIBL.TryGetValue(key, out var requestBlock))
            return;

        requestBlock.State = RequestBlockState.Cancelled;
        _requestByIBL.TryRemove(key, out _);
    }

    public ValueTask DisposeAsync()
    {
        _pendingRequests.Writer.TryComplete();
        _requestByIBL.Clear();
        return ValueTask.CompletedTask;
    }
}
