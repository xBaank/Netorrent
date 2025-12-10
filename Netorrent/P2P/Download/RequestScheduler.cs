using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Messages;
using ZLinq;

namespace Netorrent.P2P.Download;

internal class RequestScheduler(
    IReadOnlyDictionary<PeerEndpoint, PeerConnection> peers,
    Bitfield bitfield,
    PiecePicker piecePicker,
    ILogger logger
) : IRequestScheduler
{
    const int MinPeersForRarity = 6;
    const int WarmupTimeoutSecods = 8;
    const int WatchDogIntervalMiliseconds = 500;
    const int MinPeers = 6;
    const int MaxPeers = 10;

    private readonly Channel<Block> _receiveBlocksChannel = Channel.CreateBounded<Block>(
        new BoundedChannelOptions(256) { SingleWriter = false, SingleReader = true }
    );
    private readonly Channel<PeerConnection> _slotsChannel = Channel.CreateBounded<PeerConnection>(
        new BoundedChannelOptions(256) { SingleReader = true, SingleWriter = false }
    );

    private readonly HashSet<PeerConnection> _activePeers = [];
    private readonly List<PeerConnection> _interestedPeers = [];
    private readonly Lock _activePeersLock = new();
    private readonly IReadOnlyDictionary<PeerEndpoint, PeerConnection> peers = peers;
    private readonly HashSet<PeerConnection> _peersEnqueued = [];
    private readonly Lock _peersEnqueuedLock = new();

    private int _maxCurrentPeers = MinPeers;
    private CancellationTokenSource? _cts;
    private Task? _runningTask;
    private bool _disposed;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _runningTask = _cts.CancelOnFirstCompletionAndAwaitAllAsync([
            ReceiveBlocksAsync(_cts.Token),
            ReScheduleTimeoutBlocksAsync(_cts.Token),
            ProcessSlotsAsync(_cts.Token),
            SlotsWatchdogAsync(_cts.Token),
        ]);
        return _runningTask;
    }

    public async Task ProcessSlotsAsync(CancellationToken cancellationToken)
    {
        await WarmupAsync(cancellationToken).ConfigureAwait(false);

        await foreach (
            var peerConnection in _slotsChannel
                .Reader.ReadAllAsync(cancellationToken)
                .ConfigureAwait(false)
        )
        {
            // mark as dequeued as early as possible
            lock (_peersEnqueuedLock)
            {
                _peersEnqueued.Remove(peerConnection);
            }

            try
            {
                await ScheduleRequests(peerConnection, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // log if needed, but don't let exceptions kill the loop
                if(logger.IsEnabled(LogLevel.Warning))
                {
                    logger.LogWarning(
                    ex,
                    "ScheduleRequests failed for peer {Peer}",
                    peerConnection.IPEndPoint
                );
                }
                // If the peer still should be processed later, re-enqueue it:
                // await RequestSlotAsync(peerConnection, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task SlotsWatchdogAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(WatchDogIntervalMiliseconds.Milliseconds, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            // compute total outstanding requests across active peers
            int totalRequested = 0;
            List<PeerConnection> currentActive;
            lock (_activePeersLock)
            {
                currentActive = _activePeers.ToList();
            }

            foreach (var p in currentActive)
                totalRequested += p.RequestedBlocksCount; // ensure this is atomic on PeerConnection

            bool anyEnqueued;
            lock (_peersEnqueuedLock)
            {
                anyEnqueued = _peersEnqueued.Count > 0;
            }

            // If nothing is happening and no peer is enqueued, try to kick the scheduler.
            if (totalRequested == 0 && !anyEnqueued && currentActive.Count > 0)
            {
                // Re-enqueue a small subset (or all) of the active peers.
                // Use TryWrite to avoid blocking the watchdog.
                foreach (var p in currentActive)
                {
                    bool alreadyEnqueued;
                    lock (_peersEnqueuedLock)
                    {
                        alreadyEnqueued = _peersEnqueued.Contains(p);
                        if (!alreadyEnqueued)
                            _peersEnqueued.Add(p); // reserve spot
                    }

                    if (alreadyEnqueued)
                        continue;

                    // Best-effort - don't await here to avoid blocking the loop.
                    if (!_slotsChannel.Writer.TryWrite(p))
                    {
                        // If TryWrite failed, remove reservation so we can try later.
                        lock (_peersEnqueuedLock)
                        {
                            _peersEnqueued.Remove(p);
                        }
                    }
                    else
                    {
                        // Successfully re-enqueued — you may break early to avoid spamming many writes
                        // break;
                    }
                }
            }
        }
    }

    private async Task ReceiveBlocksAsync(CancellationToken cancellationToken)
    {
        await foreach (
            var receiveBlock in _receiveBlocksChannel.Reader.ReadAllAsync(cancellationToken)
        )
        {
            using var block = receiveBlock;
            await piecePicker.ReceiveBlockAsync(block, cancellationToken).ConfigureAwait(false);
            await _slotsChannel
                .Writer.WriteAsync(block.FromPeer, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private async Task ReSchedulePeersBlocksAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested) { }
    }

    private async Task ReScheduleTimeoutBlocksAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            foreach (var requestBlock in piecePicker.GetTimeoutRequestBlocks())
            {
                var lastRequestedFrom = piecePicker.GetLastRequester(requestBlock);
                piecePicker.SetBlockToPending(requestBlock);
                lastRequestedFrom.DecrementRequestedBlock();

                PeerConnection? freePeer = null;

                lock (_activePeersLock)
                {
                    foreach (var peerConnection in _activePeers)
                    {
                        if (peerConnection == lastRequestedFrom)
                            continue;
                        if (peerConnection.PeerBitField?.HasPiece(requestBlock.Index) == true)
                        {
                            freePeer = peerConnection;
                            break;
                        }
                    }
                }

                //If we can't find a peer we retry with the same one
                freePeer ??= lastRequestedFrom;
                await _slotsChannel
                    .Writer.WriteAsync(freePeer, cancellationToken)
                    .ConfigureAwait(false);
            }
            await Task.Delay(1.Seconds, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task WarmupAsync(CancellationToken cancellationToken)
    {
        var warmupDeadline = DateTime.UtcNow + WarmupTimeoutSecods.Seconds;
        while (DateTime.UtcNow < warmupDeadline && !cancellationToken.IsCancellationRequested)
        {
            int minPeersReady;
            lock (_activePeersLock)
            {
                minPeersReady = _activePeers.Count(i => i.AmInterested && !i.PeerChoking);
            }

            if (minPeersReady >= MinPeersForRarity)
                return;

            await Task.Delay(100, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask ScheduleRequests(
        PeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        if (peerConnection.PeerBitField is null)
            return;

        while (
            peerConnection.RequestedBlocksCount
            < peerConnection.PeerRequestWindow.MaxInFlightRequests
        )
        {
            var requestBlock = piecePicker.GetBlock(peerConnection.PeerBitField);

            if (requestBlock is null)
                return;

            piecePicker.SetBlockToRequested(requestBlock, peerConnection);

            try
            {
                await peerConnection
                    .SendRequestAsync(requestBlock, cancellationToken)
                    .ConfigureAwait(false);
                peerConnection.IncrementRequestedBlock();
            }
            catch (Exception ex)
            {
                if (logger.IsEnabled(LogLevel.Error))
                {
                    logger.LogError(
                        ex,
                        "Failed to send request block {Index}:{Begin} to peer {Peer}",
                        requestBlock.Index,
                        requestBlock.Begin,
                        peerConnection.IPEndPoint
                    );
                }
                peerConnection.DecrementRequestedBlock();
                break;
            }
        }
    }

    public async ValueTask RequestSlotAsync(
        PeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        bool shouldEnqueue;

        lock (_activePeersLock)
        {
            if (_activePeers.Count >= _maxCurrentPeers)
            {
                _interestedPeers.Add(peerConnection);
                return;
            }
            _activePeers.Add(peerConnection);
            shouldEnqueue = true;
        }

        if (shouldEnqueue)
        {
            await _slotsChannel
                .Writer.WriteAsync(peerConnection, cancellationToken)
                .ConfigureAwait(false);

            lock (_peersEnqueuedLock)
            {
                _peersEnqueued.Add(peerConnection);
            }
        }
    }

    public async ValueTask FreeSlotAsync(
        PeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        PeerConnection? nextPeer = null;

        lock (_activePeersLock)
        {
            _interestedPeers.Remove(peerConnection);
            _activePeers.Remove(peerConnection);

            if (_interestedPeers.Count > 0)
            {
                nextPeer = _interestedPeers[0];
                _interestedPeers.RemoveAt(0);
                _activePeers.Add(nextPeer);
            }
        }

        if (nextPeer is not null)
        {
            await _slotsChannel
                .Writer.WriteAsync(nextPeer, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    public void IncreaseRarity(int index) => piecePicker.IncreaseRarity(index);

    public void DecreaseRarity(int index) => piecePicker.DecreaseRarity(index);

    public async ValueTask ReceiveBlockAsync(Block block, CancellationToken cancellationToken)
    {
        await _receiveBlocksChannel
            .Writer.WriteOrDisposeAsync(block, cancellationToken)
            .ConfigureAwait(false);
    }

    private async ValueTask DrainChannelsAsync()
    {
        await foreach (
            var item in _receiveBlocksChannel.Reader.ReadAllAsync().ConfigureAwait(false)
        )
            item.Dispose();

        await foreach (var _ in _slotsChannel.Reader.ReadAllAsync().ConfigureAwait(false)) { }
    }

    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            _cts?.Cancel();
            _receiveBlocksChannel.Writer.TryComplete();
            _slotsChannel.Writer.TryComplete();

            try
            {
                if (_runningTask is not null)
                    await _runningTask.ConfigureAwait(false);
            }
            catch { }

            await DrainChannelsAsync().ConfigureAwait(false);
            await piecePicker.DisposeAsync().ConfigureAwait(false);
            _cts?.Dispose();
        }
    }
}
