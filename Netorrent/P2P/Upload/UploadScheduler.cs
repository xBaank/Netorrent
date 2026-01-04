using System.Threading;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Messages;
using Netorrent.Statistics;
using Netorrent.TorrentFile;
using R3;
using ZLinq;

namespace Netorrent.P2P.Upload;

//https://www.bittorrent.org/beps/bep_0003.html
//https://read.seas.harvard.edu/~kohler/pubs/legout07clustering.pdf Section 2.3

internal class UploadScheduler(
    IPieceStorage pieceStorage,
    Bitfield bitfield,
    TransferStatistics transfer,
    ILogger logger
) : IUploadScheduler
{
    const int MaxActivePeers = 4; //TODO Add an option for this to be changed

    private readonly Channel<RequestBlock> _pendingRequests = Channel.CreateBounded<RequestBlock>(
        new BoundedChannelOptions(256) { SingleWriter = false, SingleReader = true }
    );

    private readonly Dictionary<IPeerConnection, IDisposable> _interestedDisposables = [];
    private readonly HashSet<IPeerConnection> _connectedPeers = [];
    private readonly HashSet<IPeerConnection> _activePeers = [];
    private readonly SemaphoreSlim _semaphore = new(1);
    private readonly TimeSpan _interval = 10.Seconds;

    private byte _round = 1;
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
        ]);
        return _runningTask;
    }

    public async Task ProcessRegularChokingAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            await RunRoundAsync(cancellationToken).ConfigureAwait(false);
            await Task.Delay(_interval, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task RunRoundAsync(CancellationToken cancellationToken)
    {
        using (await _semaphore.LockAsync(cancellationToken).ConfigureAwait(false))
        {
            await RegularChokeAsync(cancellationToken).ConfigureAwait(false);
            _round++;

            if (_round == 3)
            {
                await OptimisticChokeAsync(cancellationToken).ConfigureAwait(false);
                _round = 1;
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

    private async ValueTask OptimisticChokeAsync(CancellationToken cancellationToken)
    {
        var peerToUnchoke = _connectedPeers
            .AsValueEnumerable()
            .Where(i => i.AmChoking.CurrentValue)
            .Where(i => i.PeerInterested.CurrentValue)
            .Shuffle()
            .FirstOrDefault();

        if (peerToUnchoke is null)
        {
            return;
        }

        if (_activePeers.Count == MaxActivePeers)
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
                await worstPeer.ChokeAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        if (_activePeers.Add(peerToUnchoke))
        {
            await peerToUnchoke.UnchokeAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    //TODO implement new choke algorithm as seeder to avoid free riders
    private async ValueTask RegularChokeAsync(CancellationToken cancellationToken)
    {
        var bestPeersByDownload = _connectedPeers
            .AsValueEnumerable()
            .OrderByDescending(i =>
                i.MyBitField.IsComplete ? i.UploadTracker.Speed.Bps : i.DownloadTracker.Speed.Bps
            )
            .ToArray();

        var toUnchokeCount = MaxActivePeers - 1;
        var activePeersCount = 0;

        foreach (var peer in bestPeersByDownload)
        {
            //Choke and ignore peers that were not active in the last 30 seconds
            if (peer.TimeSinceReceivedBlock >= 30.Seconds)
            {
                _activePeers.Remove(peer);
                await peer.ChokeAsync(cancellationToken).ConfigureAwait(false);
                continue;
            }

            //If the peer is already as active downloader we do nothing
            if (_activePeers.Contains(peer))
            {
                activePeersCount++;
                continue;
            }

            //Peer can be unchoked
            if (activePeersCount < toUnchokeCount)
            {
                await peer.UnchokeAsync(cancellationToken).ConfigureAwait(false);
            }
            //Peer can't be added so it's choked and removed if it's an active downloader
            else
            {
                await peer.ChokeAsync(cancellationToken).ConfigureAwait(false);
                _activePeers.Remove(peer);
            }

            //Peer is interested and unchoked (we add it)
            if (!peer.AmChoking.CurrentValue && peer.PeerInterested.CurrentValue)
            {
                _activePeers.Add(peer);
                activePeersCount++;
            }
        }
    }

    private async ValueTask CheckRoundAsync(
        IPeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        if (peerConnection.PeerInterested.CurrentValue && !peerConnection.AmChoking.CurrentValue)
        {
            await RunRoundAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public async ValueTask AddPeerAsync(
        IPeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        using (await _semaphore.LockAsync(cancellationToken).ConfigureAwait(false))
        {
            if (_connectedPeers.Add(peerConnection))
            {
                _interestedDisposables[peerConnection] =
                    peerConnection.PeerInterested.SubscribeAwait(
                        (_, c) => CheckRoundAsync(peerConnection, c),
                        configureAwait: false
                    );
            }
        }
    }

    public async ValueTask RemovePeerAsync(
        IPeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        using (await _semaphore.LockAsync(cancellationToken).ConfigureAwait(false))
        {
            if (_connectedPeers.Remove(peerConnection))
            {
                if (_interestedDisposables.TryGetValue(peerConnection, out var disposable))
                {
                    disposable.Dispose();
                    _interestedDisposables.Remove(peerConnection);
                }
                _activePeers.Remove(peerConnection);
            }
        }

        await CheckRoundAsync(peerConnection, cancellationToken).ConfigureAwait(false);
        await peerConnection.ChokeAsync(cancellationToken).ConfigureAwait(false);
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
        using (await _semaphore.LockAsync(cancellationToken).ConfigureAwait(false))
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
        using (await _semaphore.LockAsync(default).ConfigureAwait(false))
        {
            foreach (var item in _interestedDisposables.Values)
            {
                item.Dispose();
            }
        }
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
            _semaphore.Dispose();
        }
    }
}
