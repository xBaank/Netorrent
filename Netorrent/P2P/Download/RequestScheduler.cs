using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Messages;
using ZLinq;

namespace Netorrent.P2P.Download;

internal class RequestScheduler(
    Bitfield myBitfield,
    FileManager fileManager,
    PiecePicker piecePicker,
    ILogger logger
) : IRequestScheduler
{
    const int MinPeersForRarity = 6;
    const int WarmupTimeoutSecods = 8;
    const int TimeoutSeconds = 10;
    const int MinPeers = 6;
    const int MaxPeers = 10;

    private readonly Channel<Block> _receiveBlocksChannel = Channel.CreateBounded<Block>(
        new BoundedChannelOptions(256) { SingleWriter = false, SingleReader = true }
    );
    private readonly Channel<PeerConnection> _slotsChannel = Channel.CreateBounded<PeerConnection>(
        new BoundedChannelOptions(256) { SingleReader = true, SingleWriter = true }
    );

    private readonly ConcurrentDictionary<PeerConnection, int> _currentPieceIndexByPeer = [];
    private readonly ConcurrentDictionary<int, RequestBlock?[]> _requestBlocksByPieceIndex = [];
    private readonly ConcurrentDictionary<int, PieceBuffer> _pieceBuffers = [];
    private readonly List<PeerConnection> _activePeers = [];
    private readonly List<PeerConnection> _interestedPeers = [];
    private readonly SemaphoreSlim _activePeersSemaphore = new(1);

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
            using var unused = receiveBlock;

            if (!_pieceBuffers.TryGetValue(receiveBlock.Index, out var pieceBuffer))
            {
                pieceBuffer = new PieceBuffer(receiveBlock.Index, fileManager);
                _pieceBuffers[receiveBlock.Index] = pieceBuffer;
            }

            pieceBuffer.AddBlock(receiveBlock);

            if (_requestBlocksByPieceIndex.TryGetValue(receiveBlock.Index, out var requestBlocks))
            {
                var requestBlock = requestBlocks.FirstOrDefault(i =>
                    i?.Begin == receiveBlock.Begin
                );
                if (
                    requestBlock is not null
                    && requestBlock.RequestedFrom.Contains(receiveBlock.FromPeer)
                )
                {
                    var rtt = requestBlock.RequestedAt.HasValue
                        ? receiveBlock.ReceivedAt - requestBlock.RequestedAt.Value
                        : TimeoutSeconds.Seconds;
                    receiveBlock.FromPeer.PeerRequestWindow.CalculateWindow(
                        (long)receiveBlock.FromPeer.DownloadSpeedTracker.CurrentBps.Bps,
                        rtt
                    );
                }

                receiveBlock.FromPeer.RequestedBlocksCount--;
                requestBlock?.State = RequestBlockState.Completed;
                requestBlock?.RequestedAt = null;
                requestBlock?.RequestedFrom.Clear();
                await _slotsChannel.Writer.WriteAsync(receiveBlock.FromPeer, cancellationToken);
            }

            if (!pieceBuffer.IsComplete)
                continue;

            try
            {
                var isWritten = await pieceBuffer.WritePieceAsync(cancellationToken);
                _pieceBuffers.TryRemove(receiveBlock.Index, out _);

                if (!isWritten)
                {
                    _pieceBuffers[receiveBlock.Index] = new PieceBuffer(
                        receiveBlock.Index,
                        fileManager
                    );
                    _requestBlocksByPieceIndex.TryRemove(receiveBlock.Index, out _);
                }
                else
                {
                    myBitfield.SetPiece(receiveBlock.Index, cancellationToken);
                    _requestBlocksByPieceIndex.TryRemove(receiveBlock.Index, out _);
                }
            }
            finally
            {
                pieceBuffer.Dispose();
                _pieceBuffers.TryRemove(receiveBlock.Index, out _);
            }
        }
    }

    private async Task ReScheduleTimeoutBlocksAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            foreach (
                var requestBlock in _requestBlocksByPieceIndex
                    .Values.AsValueEnumerable()
                    .SelectMany(i => i)
                    .Where(i => i is not null)
                    .ToArray()
            )
            {
                var passedTime =
                    DateTimeOffset.UtcNow - (requestBlock!.RequestedAt ?? DateTimeOffset.UtcNow);

                if (passedTime > TimeoutSeconds.Seconds)
                {
                    var lastRequestedFrom = requestBlock.RequestedFrom[^1];
                    requestBlock.State = RequestBlockState.Pending;
                    requestBlock.RequestedAt = null;
                    lastRequestedFrom.PeerRequestWindow.CalculateWindow(
                        (long)lastRequestedFrom.DownloadSpeedTracker.CurrentBps.Bps,
                        passedTime
                    );

                    await _activePeersSemaphore.WaitAsync(cancellationToken);
                    try
                    {
                        var freePeer = _activePeers
                            .AsValueEnumerable()
                            .FirstOrDefault(i =>
                                i.AmInterested
                                && !i.PeerChocking
                                && i.RequestedBlocksCount < i.PeerRequestWindow.MaxInFlightRequests
                            );

                        if (freePeer is not null)
                        {
                            await ScheduleRequests(freePeer, cancellationToken);
                        }
                    }
                    finally
                    {
                        _activePeersSemaphore.Release();
                    }
                }

                await Task.Delay(1.Seconds, cancellationToken);
            }
        }
    }

    private async Task WarmupAsync(CancellationToken cancellationToken)
    {
        var warmupTask = Task.Delay(WarmupTimeoutSecods.Seconds, cancellationToken);
        var minPeersReady = 0;
        do
        {
            await Task.Delay(100.Milliseconds, cancellationToken);
            minPeersReady = _activePeers
                .AsValueEnumerable()
                .Count(i => i.AmInterested && !i.PeerChocking);
        } while (!warmupTask.IsCompletedSuccessfully && minPeersReady < MinPeersForRarity);
    }

    private async ValueTask ScheduleRequests(
        PeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        while (
            peerConnection.RequestedBlocksCount
            < peerConnection.PeerRequestWindow.MaxInFlightRequests
        )
        {
            if (peerConnection.PeerBitField is null)
                throw new InvalidOperationException("PeerBitfield should not be null");

            //TODO this is not ok, we should save the rarest piece index for this peer and recalculate only when all request blocks are created and sent
            if (!_currentPieceIndexByPeer.TryGetValue(peerConnection, out var pieceIndex))
            {
                var possiblePieceIndex = piecePicker.GetRarestPiece(
                    peerConnection.PeerBitField,
                    _requestBlocksByPieceIndex
                        .AsValueEnumerable()
                        .Where(i => i.Value.Any(x => x is null))
                        .Select(i => i.Key)
                        .ToArray()
                );

                //No piece can be downloaded
                if (possiblePieceIndex is null)
                    return;

                pieceIndex = possiblePieceIndex.Value;
                _currentPieceIndexByPeer[peerConnection] = pieceIndex;
            }

            var requestBlock = SelectBlock(pieceIndex);

            //All requestBlocks are already created for this piece so we try to generate a new one
            if (requestBlock is null)
            {
                _currentPieceIndexByPeer.Remove(peerConnection, out var _);
                continue;
            }

            requestBlock.State = RequestBlockState.Requested;
            requestBlock.RequestedFrom.Add(peerConnection);
            requestBlock.RequestedAt = DateTimeOffset.UtcNow;
            peerConnection.RequestedBlocksCount++;

            try
            {
                await peerConnection.SendRequestAsync(requestBlock, cancellationToken);
            }
            catch (Exception ex)
            {
                requestBlock.RequestedFrom.Remove(peerConnection);
                requestBlock.RequestedAt = null;
                requestBlock.State = RequestBlockState.Pending;
                peerConnection.RequestedBlocksCount--;
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

    private RequestBlock? SelectBlock(int index)
    {
        if (!_requestBlocksByPieceIndex.TryGetValue(index, out var requestBlocks))
        {
            var blockCount = fileManager.GetBlockCountByPieceIndex(index);
            requestBlocks = new RequestBlock[blockCount];
            _requestBlocksByPieceIndex[index] = requestBlocks;
        }

        for (int i = 0; i < requestBlocks.Length; i++)
        {
            var currentRequestBlock = requestBlocks[i];

            if (
                currentRequestBlock is not null
                && currentRequestBlock.State == RequestBlockState.Completed
            )
                continue;

            if (currentRequestBlock is null or { State: RequestBlockState.Pending })
            {
                return requestBlocks[i] = fileManager.GetRequestBlockByBlockIndex(index, i);
            }
        }

        return null;
    }

    public async ValueTask RequestSlotAsync(
        PeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        await _activePeersSemaphore.WaitAsync(cancellationToken);
        try
        {
            if (
                peerConnection.PeerBitField is null
                || !myBitfield.HasAnyMissingPiece(peerConnection.PeerBitField)
            )
                return;

            if (_activePeers.Count >= _maxCurrentPeers)
            {
                _interestedPeers.Add(peerConnection);
                return;
            }
            await _slotsChannel.Writer.WriteAsync(peerConnection, cancellationToken);
            _activePeers.Add(peerConnection);
        }
        finally
        {
            _activePeersSemaphore.Release();
        }
    }

    public async ValueTask FreeSlotAsync(
        PeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        await _activePeersSemaphore.WaitAsync(cancellationToken);
        try
        {
            _interestedPeers.Remove(peerConnection);
            _activePeers.Remove(peerConnection);

            var nextPeer = _interestedPeers.AsValueEnumerable().FirstOrDefault();
            if (nextPeer is not null)
            {
                await _slotsChannel.Writer.WriteAsync(nextPeer, cancellationToken);
                _interestedPeers.Remove(nextPeer);
            }
        }
        finally
        {
            _activePeersSemaphore.Release();
        }
    }

    public void IncreaseRarity(int index) => piecePicker.IncreaseRarity(index);

    public void DecreaseRarity(int index) => piecePicker.DecreaseRarity(index);

    public async ValueTask OnPeerUnchockedAsync(
        PeerConnection peer,
        CancellationToken cancellationToken
    ) => await _slotsChannel.Writer.WriteAsync(peer, cancellationToken);

    public async ValueTask ReceiveBlockAsync(Block block, CancellationToken cancellationToken)
    {
        await _receiveBlocksChannel.Writer.WriteOrDisposeAsync(block, cancellationToken);
    }

    private async ValueTask DrainChannelsAsync()
    {
        await foreach (var item in _receiveBlocksChannel.Reader.ReadAllAsync())
            item.Dispose();

        await foreach (var _ in _slotsChannel.Reader.ReadAllAsync()) { }

        foreach (var item in _pieceBuffers.Values)
            item.Dispose();

        await _receiveBlocksChannel.Reader.Completion;
        await _slotsChannel.Reader.Completion;
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

            _cts?.Dispose();
        }
    }
}
