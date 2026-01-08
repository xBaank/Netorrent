using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Messages;
using Netorrent.Statistics;
using ZLinq;

namespace Netorrent.P2P.Download;

internal class RequestScheduler(
    IReadOnlyDictionary<PeerEndpoint, IPeerConnection> peers,
    IPiecePicker piecePicker,
    Bitfield myBitfield,
    TransferStatistics transfer,
    TimeSpan warmupTime,
    IPieceStorage pieceStorage,
    ILogger logger
) : IRequestScheduler
{
    const int MinPeersForRarity = 6;

    private readonly Channel<Block> _receiveBlocksChannel = Channel.CreateBounded<Block>(
        new BoundedChannelOptions(256) { SingleWriter = false, SingleReader = true }
    );
    private readonly Channel<IPeerConnection> _slotsChannel =
        Channel.CreateBounded<IPeerConnection>(
            new BoundedChannelOptions(256) { SingleWriter = false, SingleReader = true }
        );

    private readonly Dictionary<int, PieceBuffer> _pieceBuffers = [];
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
            ScheduleRequests(peerConnection);
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
            await TryRequestAsync(receiveBlock.FromPeer, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask ProcessBlockAsync(Block block, CancellationToken cancellationToken)
    {
        //TODO penalize?
        if (!piecePicker.TryGetRequestedBlock(block, out var requestedBlock))
        {
            return;
        }

        //Initialize pieceBuffer
        if (!_pieceBuffers.TryGetValue(block.Index, out var pieceBuffer))
        {
            pieceBuffer = new PieceBuffer(block.Index, pieceStorage, piecePicker);
            _pieceBuffers[block.Index] = pieceBuffer;
        }

        foreach (var peerConnection in requestedBlock.RequestedFrom)
        {
            if (peerConnection != block.FromPeer)
            {
                peerConnection.TrySendCancel(requestedBlock);
            }
            peerConnection.DecrementRequestedBlock();
        }

        block.FromPeer.PeerRequestWindow.ReceivedBlock(
            (ulong)block.FromPeer.DownloadTracker.Speed.Bps
        );
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
        while (true)
        {
            var timeoutRequestBlocks = piecePicker.GetTimeoutRequestBlocks();

            if (timeoutRequestBlocks.Length == 0)
            {
                await Task.Delay(1.Seconds, cancellationToken).ConfigureAwait(false);
                continue;
            }

            foreach (var requestBlock in timeoutRequestBlocks)
            {
                var lastRequestedFrom = piecePicker.GetLastRequesterOrNull(requestBlock);
                IPeerConnection? freePeer = null;

                foreach (var peerConnection in peers.Values.AsValueEnumerable())
                {
                    if (peerConnection == lastRequestedFrom)
                    {
                        continue;
                    }

                    if (
                        peerConnection.AmInterested.CurrentValue
                        && !peerConnection.PeerChoking.CurrentValue
                    )
                    {
                        freePeer = peerConnection;
                        break;
                    }
                }

                if (freePeer is not null)
                {
                    piecePicker.SetBlockToPending(requestBlock);
                    await _slotsChannel
                        .Writer.WriteAsync(freePeer, cancellationToken)
                        .ConfigureAwait(false);
                }
            }
            await Task.Delay(1.Seconds, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task WarmupAsync(CancellationToken cancellationToken)
    {
        var warmupDeadline = DateTime.UtcNow + warmupTime;
        while (DateTime.UtcNow < warmupDeadline && !cancellationToken.IsCancellationRequested)
        {
            var minPeersReady = peers
                .Values.AsValueEnumerable()
                .Count(i => i.AmInterested.CurrentValue && !i.PeerChoking.CurrentValue);

            if (minPeersReady >= MinPeersForRarity)
            {
                return;
            }

            await Task.Delay(100, cancellationToken).ConfigureAwait(false);
        }
    }

    private void ScheduleRequests(IPeerConnection peerConnection)
    {
        if (peerConnection.PeerBitField is null)
        {
            return;
        }

        while (
            peerConnection.RequestedBlocksCount
            < peerConnection.PeerRequestWindow.MaxInFlightRequests
        )
        {
            var requestBlock = piecePicker.GetBlock(peerConnection.PeerBitField);

            if (requestBlock is null)
            {
                return;
            }

            if (peerConnection.TrySendRequest(requestBlock))
            {
                piecePicker.SetBlockToRequested(requestBlock, peerConnection);
                peerConnection.IncrementRequestedBlock();
            }
            else
            {
                if (logger.IsEnabled(LogLevel.Information))
                {
                    logger.LogInformation(
                        "Failed to send request block {Index}:{Begin} to peer {Peer} with {blocks}/{maxblocks}",
                        requestBlock.Index,
                        requestBlock.Begin,
                        peerConnection.PeerEndpoint.PeerId,
                        peerConnection.RequestedBlocksCount,
                        peerConnection.PeerRequestWindow.MaxInFlightRequests
                    );
                }
                return;
            }
        }
    }

    public async ValueTask TryRequestAsync(
        IPeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        if (!peerConnection.PeerChoking.CurrentValue && peerConnection.AmInterested.CurrentValue)
        {
            await _slotsChannel
                .Writer.WriteAsync(peerConnection, cancellationToken)
                .ConfigureAwait(false);
        }
    }

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
        {
            item.Dispose();
        }
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
