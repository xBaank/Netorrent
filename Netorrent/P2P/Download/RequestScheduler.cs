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
    private int _maxCurrentPeers = MinPeers;
    private CancellationTokenSource? _cts;
    private Task? _runningTask;
    private bool _disposed;
    private readonly Stopwatch stopwatch = new Stopwatch();

    public Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        stopwatch.Start();
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _runningTask = _cts.CancelOnFirstCompletionAndAwaitAllAsync([
            ReceiveBlocksAsync(_cts.Token),
            ReScheduleTimeoutBlocksAsync(_cts.Token),
            ProcessSlotsAsync(_cts.Token),
        ]);
        return _runningTask;
    }

    public async Task ProcessSlotsAsync(CancellationToken cancellationToken)
    {
        await WarmupAsync(cancellationToken);
        await foreach (var peerConnection in _slotsChannel.Reader.ReadAllAsync(cancellationToken))
        {
            await ScheduleRequests(peerConnection, cancellationToken);
        }
    }

    private async Task ReceiveBlocksAsync(CancellationToken cancellationToken)
    {
        await foreach (
            var receiveBlock in _receiveBlocksChannel.Reader.ReadAllAsync(cancellationToken)
        )
        {
            using var block = receiveBlock;
            await piecePicker.ReceiveBlockAsync(block, cancellationToken);
            await _slotsChannel.Writer.WriteAsync(block.FromPeer, cancellationToken);
        }
    }

    private async Task ReScheduleTimeoutBlocksAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            foreach (var (requestBlock, passedTime) in piecePicker.GetTimeoutRequestBlocks())
            {
                var lastRequestedFrom = requestBlock.RequestedFrom[^1];

                requestBlock.State = RequestBlockState.Pending;
                requestBlock.RequestedAt = null;
                lastRequestedFrom.DecrementRequestedBlock();

                PeerConnection? freePeer = null;

                lock (_activePeersLock)
                {
                    foreach (var peerConnection in _activePeers)
                    {
                        if (peerConnection == lastRequestedFrom)
                            continue;
                        if (
                            peerConnection.RequestedBlocksCount
                            < peerConnection.PeerRequestWindow.MaxInFlightRequests
                        )
                        {
                            freePeer = peerConnection;
                            break;
                        }
                    }
                }

                //If we can't find a peer we retry with the same one
                freePeer ??= lastRequestedFrom;
                logger.LogInformation(
                    "Requesting timedout to {peer} block with {time} with total time of {total}",
                    freePeer.PeerId,
                    passedTime.TotalSeconds,
                    stopwatch.Elapsed.TotalSeconds
                );
                await _slotsChannel.Writer.WriteAsync(freePeer, cancellationToken);
            }
            await Task.Delay(1.Seconds, cancellationToken);
        }
    }

    private async Task WarmupAsync(CancellationToken cancellationToken)
    {
        var warmupTask = Task.Delay(WarmupTimeoutSecods.Seconds, cancellationToken);
        var minPeersReady = 0;
        do
        {
            await Task.Delay(100.Milliseconds, cancellationToken);
            lock (_activePeersLock)
            {
                minPeersReady = _activePeers
                    .AsValueEnumerable()
                    .Count(i => i.AmInterested && !i.PeerChoking);
            }
        } while (!warmupTask.IsCompleted && minPeersReady < MinPeersForRarity);
    }

    private async ValueTask ScheduleRequests(
        PeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        if (peerConnection.PeerBitField is null)
            throw new InvalidOperationException("PeerBitfield should not be null");

        var possiblePieceIndex = piecePicker.GetPiece(peerConnection.PeerBitField);

        while (
            peerConnection.RequestedBlocksCount
            < peerConnection.PeerRequestWindow.MaxInFlightRequests
        )
        {
            if (possiblePieceIndex is null)
                return;

            var requestBlock = piecePicker.SelectBlock(possiblePieceIndex.Value);

            //All requestBlocks are already created for this piece so we try to generate a new one
            if (requestBlock is null)
            {
                // try to find another piece once; avoid hot spin if we get the same piece again
                var newIndex = piecePicker.GetPiece(peerConnection.PeerBitField);

                if (newIndex == possiblePieceIndex || newIndex is null)
                {
                    logger.LogInformation("No piece selected");
                    return;
                }

                possiblePieceIndex = newIndex;
                continue;
            }

            requestBlock.State = RequestBlockState.Requested;
            requestBlock.RequestedFrom.Add(peerConnection);
            requestBlock.RequestedAt = DateTimeOffset.UtcNow;
            peerConnection.IncrementRequestedBlock();

            try
            {
                await peerConnection.SendRequestAsync(requestBlock, cancellationToken);
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
            await _slotsChannel.Writer.WriteAsync(peerConnection, cancellationToken);
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
            await _slotsChannel.Writer.WriteAsync(nextPeer, cancellationToken);
        }
    }

    public void IncreaseRarity(int index) => piecePicker.IncreaseRarity(index);

    public void DecreaseRarity(int index) => piecePicker.DecreaseRarity(index);

    public async ValueTask ReceiveBlockAsync(Block block, CancellationToken cancellationToken)
    {
        await _receiveBlocksChannel.Writer.WriteOrDisposeAsync(block, cancellationToken);
    }

    private async ValueTask DrainChannelsAsync()
    {
        await foreach (var item in _receiveBlocksChannel.Reader.ReadAllAsync())
            item.Dispose();

        await foreach (var _ in _slotsChannel.Reader.ReadAllAsync()) { }
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
                    await _runningTask;
            }
            catch { }

            await DrainChannelsAsync();
            await piecePicker.DisposeAsync();
            _cts?.Dispose();
        }
    }
}
