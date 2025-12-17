using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Messages;
using Netorrent.Statistics;

namespace Netorrent.P2P.Upload;

internal class UploadScheduler(
    IPieceStorage pieceStorage,
    Bitfield bitfield,
    TransferStatistics transfer,
    ILogger logger
) : IUploadScheduler
{
    const int MaxInFlightUploadRequests = 4;
    const int MaxUnchokedPeers = 4;

    private readonly Channel<RequestBlock> _pendingRequests = Channel.CreateBounded<RequestBlock>(
        new BoundedChannelOptions(256) { SingleWriter = false, SingleReader = true }
    );
    private readonly Channel<PeerConnection> _slotsRequests = Channel.CreateBounded<PeerConnection>(
        new BoundedChannelOptions(256) { SingleWriter = false, SingleReader = true }
    );

    private readonly List<PeerConnection> _interestedPeers = [];
    private readonly HashSet<PeerConnection> _unchokedPeers = [];
    private readonly Lock _unchokedSlotsLock = new();

    private CancellationTokenSource? _cts;
    private Task? _runningTask;
    private bool _disposed;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _runningTask = _cts.CancelOnFirstCompletionAndAwaitAllAsync([
            ProcessRequestsAsync(_cts.Token),
            ProcessSlotsAsync(_cts.Token),
        ]);
        return _runningTask;
    }

    public async Task ProcessSlotsAsync(CancellationToken cancellationToken)
    {
        await foreach (
            var peerConnection in _slotsRequests
                .Reader.ReadAllAsync(cancellationToken)
                .ConfigureAwait(false)
        )
        {
            try
            {
                await peerConnection.SendUnchokedAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (logger.IsEnabled(LogLevel.Error))
                {
                    logger.LogError(
                        ex,
                        "Error unchoking peer {peeid}",
                        peerConnection.PeerEndpoint.PeerId
                    );
                }
            }
        }
    }

    public async Task ProcessRequestsAsync(CancellationToken cancellationToken)
    {
        await foreach (
            var requestBlock in _pendingRequests
                .Reader.ReadAllAsync(cancellationToken)
                .ConfigureAwait(false)
        )
        {
            if (
                requestBlock.State == RequestBlockState.Cancelled
                || requestBlock.RequestedFrom.Count == 0
            )
                continue;

            var peer = requestBlock.RequestedFrom[0];

            try
            {
                var pieceData = await pieceStorage
                    .ReadAsync(
                        requestBlock.Index,
                        requestBlock.Begin,
                        requestBlock.Length,
                        cancellationToken
                    )
                    .ConfigureAwait(false);

                using var block = new Block(
                    requestBlock.Index,
                    requestBlock.Begin,
                    pieceData,
                    peer
                );
                await peer.SendBlockAsync(block, cancellationToken).ConfigureAwait(false);
                transfer.AddUploadedBytes(block.Payload.Length);
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
            finally
            {
                peer.DecrementUploadRequested();
            }
        }
    }

    public async ValueTask RequestSlotAsync(
        PeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        bool shouldEnqueue;

        lock (_unchokedSlotsLock)
        {
            if (_unchokedPeers.Count >= MaxUnchokedPeers)
            {
                _interestedPeers.Add(peerConnection);
                return;
            }

            _unchokedPeers.Add(peerConnection);
            shouldEnqueue = true;
        }

        if (shouldEnqueue)
        {
            await _slotsRequests
                .Writer.WriteAsync(peerConnection, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    public async ValueTask FreeSlotAsync(
        PeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        PeerConnection? nextPeer = null;

        lock (_unchokedSlotsLock)
        {
            _interestedPeers.Remove(peerConnection);
            _unchokedPeers.Remove(peerConnection);
            if (_interestedPeers.Count > 0)
            {
                nextPeer = _interestedPeers[0];
                _interestedPeers.RemoveAt(0);
                _unchokedPeers.Add(nextPeer);
            }
        }

        if (nextPeer is not null)
        {
            await _slotsRequests
                .Writer.WriteAsync(nextPeer, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    public async ValueTask AddRequestAsync(
        RequestBlock request,
        CancellationToken cancellationToken
    )
    {
        var from = request.RequestedFrom[0];

        lock (_unchokedSlotsLock)
        {
            if (!bitfield.HasPiece(request.Index))
            {
                if (logger.IsEnabled(LogLevel.Information))
                {
                    logger.LogInformation(
                        "Peer {peer} requested a block we don't have",
                        from.PeerEndpoint.PeerId
                    );
                }

                return;
            }
            if (!_unchokedPeers.Contains(from))
            {
                if (logger.IsEnabled(LogLevel.Information))
                {
                    logger.LogInformation("Peer {peer} is not unchoked", from.PeerEndpoint.PeerId);
                }

                return;
            }

            if (from.UploadRequestedBlocksCount >= MaxInFlightUploadRequests)
            {
                if (logger.IsEnabled(LogLevel.Information))
                {
                    logger.LogInformation(
                        "Peer {peer} reached the max requests",
                        from.PeerEndpoint.PeerId
                    );
                }
                return;
            }
        }

        from.IncrementUploadRequested();
        await _pendingRequests.Writer.WriteAsync(request, cancellationToken).ConfigureAwait(false);
    }

    public void CancelRequest(RequestBlock request)
    {
        var from = request.RequestedFrom[0];

        request.State = RequestBlockState.Cancelled;
        from.DecrementUploadRequested();
    }

    private async ValueTask DrainChannelsAsync()
    {
        await foreach (var _ in _pendingRequests.Reader.ReadAllAsync().ConfigureAwait(false)) { }
        await foreach (var _ in _slotsRequests.Reader.ReadAllAsync().ConfigureAwait(false)) { }
    }

    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            _cts?.Cancel();
            _pendingRequests.Writer.TryComplete();
            _slotsRequests.Writer.TryComplete();

            try
            {
                if (_runningTask is not null)
                    await _runningTask.ConfigureAwait(false);
            }
            catch { }

            await DrainChannelsAsync().ConfigureAwait(false);
            _cts?.Dispose();
        }
    }
}
