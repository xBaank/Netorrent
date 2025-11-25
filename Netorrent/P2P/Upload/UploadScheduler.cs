using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.IO;
using Netorrent.P2P.Messages;

namespace Netorrent.P2P.Upload;

internal class UploadScheduler(FileManager fileManager, ILogger logger) : IAsyncDisposable
{
    private readonly Channel<RequestBlock> _pendingRequests = Channel.CreateBounded<RequestBlock>(
        new BoundedChannelOptions(256) { SingleWriter = false, SingleReader = true }
    );
    private readonly ConcurrentDictionary<
        (int Index, int Begin, int Length),
        RequestBlock
    > _requestByIBL = [];

    private readonly Lock _unchokedSlotsLock = new();
    private int _unchokedSlots = 0;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await foreach (var requestBlock in _pendingRequests.Reader.ReadAllAsync(cancellationToken))
        {
            if (
                requestBlock.State == RequestBlockState.Cancelled
                || requestBlock.RequestedFrom.Count == 0
            )
                continue;

            var peer = requestBlock.RequestedFrom[0];

            _requestByIBL.TryRemove(
                (requestBlock.Index, requestBlock.Begin, requestBlock.Length),
                out _
            );

            var pieceData = await fileManager.ReadPieceAsync(
                requestBlock.Index,
                requestBlock.Begin,
                requestBlock.Length,
                cancellationToken
            );

            using var block = new Block(requestBlock.Index, requestBlock.Begin, pieceData, peer);

            try
            {
                await peer.SendBlockAsync(block, cancellationToken);
            }
            catch (Exception ex)
            {
                if (logger.IsEnabled(LogLevel.Error))
                {
                    logger.LogError(
                        ex,
                        "Failed to send block Index: {Index}, Begin: {Begin}, Length: {Length} to Peer: {PeerEndPoint}",
                        requestBlock.Index,
                        requestBlock.Begin,
                        requestBlock.Length,
                        peer
                    );
                }
            }
        }
    }

    public bool AddChokedSlot()
    {
        lock (_unchokedSlotsLock)
        {
            if (_unchokedSlots >= 4)
                return false;

            _unchokedSlots++;
            return true;
        }
    }

    public bool RemoveChokedSlot()
    {
        lock (_unchokedSlotsLock)
        {
            if (_unchokedSlots <= 0)
                return false;

            _unchokedSlots--;
            return true;
        }
    }

    public async ValueTask<bool> AddRequestAsync(
        RequestBlock request,
        CancellationToken cancellationToken
    )
    {
        var from = request.RequestedFrom[0];

        if (from.UploadRequestedBlocksCount >= 4)
            return false;

        var key = (request.Index, request.Begin, request.Length);
        _requestByIBL.TryAdd(key, request);
        await _pendingRequests.Writer.WriteAsync(request, cancellationToken);
        return true;
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
