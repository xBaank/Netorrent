using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Messages;
using Netorrent.Statistics;
using ZLinq;

namespace Netorrent.P2P.Upload;

internal class UploadScheduler(
    IPieceStorage pieceStorage,
    Bitfield bitfield,
    TransferStatistics transfer,
    ILogger logger
) : IUploadScheduler
{
    const int MaxUnchokedPeers = 4;

    private readonly Channel<RequestBlock> _pendingRequests = Channel.CreateBounded<RequestBlock>(
        new BoundedChannelOptions(256) { SingleWriter = false, SingleReader = true }
    );

    private readonly HashSet<IPeerConnection> _interestedPeers = [];
    private readonly HashSet<IPeerConnection> _unchokedPeers = [];
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
            ProcessRegularChokingAsync(_cts.Token),
            ProcessOptimisticChokingAsync(_cts.Token),
        ]);
        return _runningTask;
    }

    public async Task ProcessRegularChokingAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            RegularChoke();
            await Task.Delay(10.Seconds, cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task ProcessOptimisticChokingAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            OptimisticChoke();
            await Task.Delay(30.Seconds).ConfigureAwait(false);
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
            var pieceData = await pieceStorage
                .ReadAsync(
                    requestBlock.Index,
                    requestBlock.Begin,
                    requestBlock.Length,
                    cancellationToken
                )
                .ConfigureAwait(false);

            try
            {
                using var block = new Block(
                    requestBlock.Index,
                    requestBlock.Begin,
                    pieceData,
                    peer
                );

                if (peer.TrySendBlock(block))
                {
                    transfer.AddUploadedBytes(block.Payload.Length); //TODO Move this to message stream after data if flushed
                }
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

    private void OptimisticChoke()
    {
        lock (_unchokedSlotsLock)
        {
            if (_interestedPeers.Count == 0)
            {
                return;
            }

            //Random peer to unchoke
            var peerToUnchoke = _interestedPeers.AsValueEnumerable().Shuffle().First();
            var worstPeer = _unchokedPeers
                .AsValueEnumerable()
                .OrderBy(i => i.DownloadSpeedTracker.CurrentBps.Bps)
                .FirstOrDefault();

            if (worstPeer is not null)
            {
                _unchokedPeers.Remove(worstPeer);
                _interestedPeers.Add(worstPeer);
                worstPeer.TrySendChoked();
            }

            _unchokedPeers.Add(peerToUnchoke);
            _interestedPeers.Remove(peerToUnchoke);
            peerToUnchoke.TrySendUnchoked();
        }
    }

    private void RegularChoke()
    {a
        lock (_unchokedSlotsLock)
        {
            var bestPeersByDownload = _unchokedPeers
                .AsValueEnumerable()
                .Concat(_interestedPeers)
                .OrderByDescending(i => i.DownloadSpeedTracker.CurrentBps.Bps)
                .Take(3)
                .ToHashSet();

            var toChoke = _unchokedPeers
                .AsValueEnumerable()
                .Where(i => !bestPeersByDownload.Contains(i));

            var toUnchoke = bestPeersByDownload
                .AsValueEnumerable()
                .Where(i => !_unchokedPeers.Contains(i));

            foreach (var peer in toChoke)
            {
                _unchokedPeers.Remove(peer);
                _interestedPeers.Add(peer);
                peer.TrySendChoked();
            }

            foreach (var peer in toUnchoke)
            {
                _unchokedPeers.Add(peer);
                _interestedPeers.Remove(peer);
                peer.TrySendUnchoked();
            }
        }
    }

    public ValueTask RequestSlotAsync(
        IPeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        lock (_unchokedSlotsLock)
        {
            if (!_unchokedPeers.Contains(peerConnection))
            {
                _interestedPeers.Add(peerConnection);
            }
        }

        RegularChoke();
        return ValueTask.CompletedTask;
    }

    public ValueTask FreeSlotAsync(
        IPeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        lock (_unchokedSlotsLock)
        {
            _unchokedPeers.Remove(peerConnection);
            _interestedPeers.Remove(peerConnection);
            peerConnection.TrySendChoked();
        }

        RegularChoke();
        return ValueTask.CompletedTask;
    }

    public async ValueTask AddRequestAsync(
        RequestBlock request,
        CancellationToken cancellationToken
    )
    {
        var from = request.RequestedFrom[0];

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
        lock (_unchokedSlotsLock)
        {
            if (!_unchokedPeers.Contains(from))
            {
                if (logger.IsEnabled(LogLevel.Information))
                {
                    logger.LogInformation("Peer {peer} is not unchoked", from.PeerEndpoint.PeerId);
                }

                return;
            }
        }

        from.IncrementUploadRequested();
        await _pendingRequests.Writer.WriteAsync(request, cancellationToken).ConfigureAwait(false);
    }

    //TODO properly implement cancellation
    public void CancelRequest(RequestBlock request)
    {
        var from = request.RequestedFrom[0];

        request.State = RequestBlockState.Cancelled;
        from.DecrementUploadRequested();
    }

    private async ValueTask DrainChannelsAsync()
    {
        await foreach (var _ in _pendingRequests.Reader.ReadAllAsync().ConfigureAwait(false)) { }
    }

    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            _cts?.Cancel();
            _pendingRequests.Writer.TryComplete();

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
