using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Messages;
using Netorrent.Statistics;
using ZLinq;

namespace Netorrent.P2P.Upload;

//https://www.bittorrent.org/beps/bep_0003.html
internal class UploadScheduler(
    IPieceStorage pieceStorage,
    Bitfield bitfield,
    TransferStatistics transfer,
    ILogger logger
) : IUploadScheduler
{
    const int MaxActivePeers = 4;

    private readonly Channel<RequestBlock> _pendingRequests = Channel.CreateBounded<RequestBlock>(
        new BoundedChannelOptions(256) { SingleWriter = false, SingleReader = true }
    );

    private readonly HashSet<IPeerConnection> _connectedPeers = [];
    private readonly HashSet<IPeerConnection> _activePeers = [];
    private readonly Lock _lock = new();

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
            await Task.Delay(30.Seconds, cancellationToken).ConfigureAwait(false);
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
                    transfer.AddUploadedBytes(block.Payload.Length); //TODO Move this to message stream after data if flushed ?
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
        lock (_lock)
        {
            var intersestedPeers = _connectedPeers
                .AsValueEnumerable()
                .Where(i => i.AmChoking.CurrentValue)
                .Where(i => i.PeerInterested.CurrentValue);

            var newestInterestedPeers = intersestedPeers
                .Where(i => i.ConnectionDuration <= 30.Seconds)
                .ToArray();

            //New peers get x3 times chances
            IPeerConnection[] possiblePeersToUnchoke =
            [
                .. newestInterestedPeers,
                .. newestInterestedPeers,
                .. intersestedPeers,
            ];

            var peerToUnchoke = possiblePeersToUnchoke.Shuffle().FirstOrDefault();

            if (peerToUnchoke is null)
            {
                return;
            }

            if (peerToUnchoke.PeerInterested.CurrentValue && _activePeers.Count == MaxActivePeers)
            {
                var worstPeer = _activePeers
                    .AsValueEnumerable()
                    .OrderBy(i =>
                        i.MyBitField.IsComplete
                            ? i.UploadTracker.Speed.Bps
                            : i.DownloadTracker.Speed.Bps
                    )
                    .First();

                if (_activePeers.Remove(worstPeer))
                {
                    worstPeer.TrySendChoked();
                }
            }

            if (_activePeers.Add(peerToUnchoke))
            {
                peerToUnchoke.TrySendUnchoked();
            }
        }
    }

    private void RegularChoke()
    {
        lock (_lock)
        {
            var bestPeersByDownload = _connectedPeers
                .AsValueEnumerable()
                .OrderByDescending(i =>
                    i.MyBitField.IsComplete
                        ? i.UploadTracker.Speed.Bps
                        : i.DownloadTracker.Speed.Bps
                )
                .ToHashSet();

            var toChoke = _activePeers
                .AsValueEnumerable()
                .Where(i => !bestPeersByDownload.Contains(i));

            var toUnchoke = bestPeersByDownload
                .AsValueEnumerable()
                .Where(i => !_activePeers.Contains(i));

            foreach (var peer in toChoke)
            {
                if (_activePeers.Remove(peer))
                {
                    peer.TrySendChoked();
                }
            }

            foreach (var peer in toUnchoke)
            {
                if (peer.PeerInterested.CurrentValue && peer.TrySendUnchoked())
                {
                    _activePeers.Add(peer);
                }
            }
        }
    }

    public void AddPeer(IPeerConnection peerConnection)
    {
        lock (_lock)
        {
            _connectedPeers.Add(peerConnection);
        }
    }

    public void RemovePeer(IPeerConnection peerConnection)
    {
        lock (_lock)
        {
            _connectedPeers.Remove(peerConnection);
            peerConnection.TrySendChoked();
        }
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
        lock (_lock)
        {
            //TODO penalize?
            if (!_connectedPeers.TryGetValue(from, out var peer) || peer.AmChoking.CurrentValue)
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
