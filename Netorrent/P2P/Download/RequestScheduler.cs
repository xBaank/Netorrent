using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Messages;
using Netorrent.Statistics;
using ZLinq;

namespace Netorrent.P2P.Download;

internal class RequestScheduler(
    PiecePicker piecePicker,
    Bitfield myBitfield,
    TransferStatistics transfer,
    IPieceStorage pieceStorage,
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
    private readonly Dictionary<int, PieceBuffer> _pieceBuffers = [];
    private readonly Lock _activePeersLock = new();
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
            await ScheduleRequests(peerConnection, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task ReceiveBlocksAsync(CancellationToken cancellationToken)
    {
        await foreach (
            var receiveBlock in _receiveBlocksChannel.Reader.ReadAllAsync(cancellationToken)
        )
        {
            using var block = receiveBlock;
            await ProcessBlockAsync(block, cancellationToken).ConfigureAwait(false);
            await _slotsChannel
                .Writer.WriteAsync(block.FromPeer, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private async ValueTask ProcessBlockAsync(Block block, CancellationToken cancellationToken)
    {
        if (!piecePicker.TryGetRequestedBlock(block, out var requestedBlock))
        {
            return;
        }

        if (!_pieceBuffers.TryGetValue(block.Index, out var pieceBuffer))
        {
            pieceBuffer = new PieceBuffer(block.Index, pieceStorage, piecePicker);
            _pieceBuffers[block.Index] = pieceBuffer;
        }

        var rtt = requestedBlock.RequestedAt.HasValue
            ? block.ReceivedAt - requestedBlock.RequestedAt.Value
            : PiecePicker.TimeoutSeconds.Seconds;
        block.FromPeer.PeerRequestWindow.CalculateRtt(rtt);
        block.FromPeer.DecrementRequestedBlock();
        piecePicker.CompleteRequestBlock(requestedBlock);
        pieceBuffer.AddBlock(block);

        if (!pieceBuffer.IsComplete)
        {
            return;
        }

        var isWritten = false;
        try
        {
            isWritten = await pieceBuffer.WritePieceAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            piecePicker.CompletePiece(block.Index);
            if (_pieceBuffers.Remove(block.Index, out var removedBuffer))
            {
                removedBuffer.Dispose();
            }
        }

        if (isWritten)
        {
            if (!myBitfield.HasPiece(block.Index))
            {
                transfer.AddVerifiedBytes(pieceBuffer.Size);
                myBitfield.SetPiece(block.Index);
            }
        }
        else
        {
            // Retry with a fresh buffer
            _pieceBuffers[block.Index] = new PieceBuffer(block.Index, pieceStorage, piecePicker);
            transfer.AddDiscardedBytes(pieceBuffer.Size);
        }
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
                        peerConnection.PeerEndpoint.PeerId
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

        foreach (var item in _pieceBuffers.Values)
        {
            item.Dispose();
        }
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
