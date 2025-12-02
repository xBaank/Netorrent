using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Messages;

namespace Netorrent.P2P.Upload;

internal class UploadScheduler(FileManager fileManager, ILogger logger) : IUploadScheduler
{
    const int MaxInFlightUploadRequests = 4;
    private readonly Channel<RequestBlock> _pendingRequests = Channel.CreateBounded<RequestBlock>(
        new BoundedChannelOptions(256) { SingleWriter = false, SingleReader = true }
    );
    private readonly Channel<PeerConnection> _slotsRequests = Channel.CreateBounded<PeerConnection>(
        new BoundedChannelOptions(16) { SingleWriter = true, SingleReader = true }
    );
    private readonly ConcurrentDictionary<
        (int Index, int Begin, int Length),
        RequestBlock
    > _requestByIBL = [];

    private readonly List<PeerConnection> _interestedPeers = [];

    private readonly SemaphoreSlim _unchokedSlotsSemahpore = new(1);
    private int _unchokedSlots = 0;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        await cts.CancelOnFirstCompletionAndAwaitAllAsync([
            ProcessRequestsAsync(cts.Token),
            ProcessSlotsAsync(cts.Token),
        ]);
    }

    public async Task ProcessSlotsAsync(CancellationToken cancellationToken)
    {
        await foreach (var peerConnection in _slotsRequests.Reader.ReadAllAsync(cancellationToken))
        {
            await peerConnection.SendUnchokedAsync(cancellationToken);
        }
    }

    public async Task ProcessRequestsAsync(CancellationToken cancellationToken)
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

    public async ValueTask RequestSlotAsync(
        PeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        await _unchokedSlotsSemahpore.WaitAsync(cancellationToken);
        try
        {
            if (_unchokedSlots >= 4)
            {
                _interestedPeers.Add(peerConnection);
                return;
            }

            await _slotsRequests.Writer.WriteAsync(peerConnection, cancellationToken);
            _unchokedSlots++;
            return;
        }
        finally
        {
            _unchokedSlotsSemahpore.Release();
        }
    }

    public async ValueTask FreeSlotAsync(
        PeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        await _unchokedSlotsSemahpore.WaitAsync(cancellationToken);
        try
        {
            if (_unchokedSlots > 0)
                _unchokedSlots--;

            _interestedPeers.Remove(peerConnection);

            var nextPeer = _interestedPeers.FirstOrDefault();
            if (nextPeer is not null)
            {
                await _slotsRequests.Writer.WriteAsync(nextPeer, cancellationToken);
                _interestedPeers.Remove(nextPeer);
            }
        }
        finally
        {
            _unchokedSlotsSemahpore.Release();
        }
    }

    public async ValueTask<bool> AddRequestAsync(
        RequestBlock request,
        CancellationToken cancellationToken
    )
    {
        var from = request.RequestedFrom[0];

        if (from.UploadRequestedBlocksCount >= MaxInFlightUploadRequests)
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
        _slotsRequests.Writer.TryComplete();
        _unchokedSlotsSemahpore.Dispose();
        _requestByIBL.Clear();
        return ValueTask.CompletedTask;
    }
}
