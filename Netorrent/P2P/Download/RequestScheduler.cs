using System.Collections.Concurrent;
using System.Net;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Messages;
using ZLinq;

namespace Netorrent.P2P.Download;

internal class RequestScheduler(
    IReadOnlyDictionary<PeerEndpoint, PeerConnection> activePeers,
    Bitfield myBitfield,
    FileManager fileManager,
    ILogger logger
) : IRequestScheduler
{
    const int MinPeersForRarity = 6;
    const int WarmupTimeoutSecods = 8;
    const int TimeoutSeconds = 10;

    private readonly Channel<Block> _receiveBlocksChannel = Channel.CreateBounded<Block>(
        new BoundedChannelOptions(256) { SingleWriter = false, SingleReader = true }
    );
    private readonly Channel<PeerConnection> _scheduleChannel =
        Channel.CreateBounded<PeerConnection>(
            new BoundedChannelOptions(256) { SingleReader = true, SingleWriter = false }
        );

    private readonly Lock _rarityLock = new();
    private readonly int[] _pieceRarity = new int[myBitfield.Length];
    private readonly ConcurrentDictionary<int, RequestBlock?[]> _requestBlocksByPieceIndex = [];
    private readonly ConcurrentDictionary<int, PieceBuffer> _pieceBuffers = [];
    private readonly HashSet<int> _currentRarestPieces = [];

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        List<Task> tasks =
        [
            ReceiveBlocksAsync(cts.Token),
            ReScheduleTimeoutBlocksAsync(cts.Token),
            SchedulePiecesAsync(cts.Token),
        ];
        var finishedTask = await Task.WhenAny(tasks);
        _receiveBlocksChannel.Writer.TryComplete();
        _scheduleChannel.Writer.TryComplete();
        cts.Cancel();
        await Task.WhenAll(tasks);

        while (_receiveBlocksChannel.Reader.TryRead(out var leftover))
        {
            leftover.Dispose();
        }

        foreach (var item in _pieceBuffers)
        {
            item.Value.Dispose();
        }

        await finishedTask;
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
                await _scheduleChannel.Writer.WriteAsync(receiveBlock.FromPeer, cancellationToken);
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
                    lock (_rarityLock)
                    {
                        _currentRarestPieces.Remove(receiveBlock.Index);
                    }
                    _requestBlocksByPieceIndex.TryRemove(receiveBlock.Index, out _);
                    myBitfield.SetPiece(receiveBlock.Index, cancellationToken);
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

                    var freePeer = activePeers
                        .Values.AsValueEnumerable()
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
            }

            await Task.Delay(1.Seconds, cancellationToken);
        }
    }

    private async Task SchedulePiecesAsync(CancellationToken cancellationToken)
    {
        await WarmupAsync(cancellationToken);
        await foreach (
            var peerConnection in _scheduleChannel.Reader.ReadAllAsync(cancellationToken)
        )
        {
            lock (_rarityLock)
            {
                var rarestCount = Math.Min(_pieceRarity.Length / 10, 500);
                var toTake = rarestCount / 2;

                var rarestPieces = _pieceRarity
                    .AsValueEnumerable()
                    .Select((rarity, index) => new { Index = index, Rarity = rarity })
                    .Where(i => !myBitfield.HasPiece(i.Index))
                    .OrderByDescending(i => i.Rarity)
                    .Take(rarestCount)
                    .Shuffle()
                    .Take(toTake)
                    .Select(i => i.Index)
                    .ToHashSet();

                foreach (var pieceIndex in rarestPieces)
                {
                    if (_currentRarestPieces.Contains(pieceIndex))
                        continue;

                    _currentRarestPieces.Add(pieceIndex);
                }
            }

            if (!peerConnection.AmInterested || peerConnection.PeerChocking)
                continue;

            await ScheduleRequests(peerConnection, cancellationToken);
        }
    }

    private async Task WarmupAsync(CancellationToken cancellationToken)
    {
        var warmupTask = Task.Delay(WarmupTimeoutSecods.Seconds, cancellationToken);
        var minPeersReady = 0;
        do
        {
            await Task.Delay(100.Milliseconds, cancellationToken);
            minPeersReady = activePeers.Values.Count(i => i.AmInterested && !i.PeerChocking);
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
            var block = SelectBlock(peerConnection);

            if (block is null)
                break;

            block.State = RequestBlockState.Requested;
            block.RequestedFrom.Add(peerConnection);
            block.RequestedAt = DateTimeOffset.UtcNow;
            peerConnection.RequestedBlocksCount++;

            try
            {
                await peerConnection.SendRequestAsync(block, cancellationToken);
            }
            catch (Exception ex)
            {
                block.RequestedFrom.Remove(peerConnection);
                block.RequestedAt = null;
                block.State = RequestBlockState.Pending;
                peerConnection.RequestedBlocksCount--;
                if (logger.IsEnabled(LogLevel.Error))
                {
                    logger.LogError(
                        ex,
                        "Failed to send request block {Index}:{Begin} to peer {Peer}",
                        block.Index,
                        block.Begin,
                        peerConnection.IPEndPoint
                    );
                }
                break;
            }
        }
    }

    private RequestBlock? SelectBlock(PeerConnection peerConnection)
    {
        lock (_rarityLock)
        {
            if (_currentRarestPieces is null)
                return null;

            foreach (var index in _currentRarestPieces)
            {
                if (myBitfield.HasPiece(index))
                    continue;

                if (!peerConnection.PeerBitField.HasPiece(index))
                    continue;

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
            }
        }

        return null;
    }

    public void IncreaseRarity(int index)
    {
        Interlocked.Increment(ref _pieceRarity[index]);
    }

    public void DecreaseRarity(int index)
    {
        Interlocked.Decrement(ref _pieceRarity[index]);
    }

    public async ValueTask OnPeerUnchockedAsync(
        PeerConnection peer,
        CancellationToken cancellationToken
    ) => await _scheduleChannel.Writer.WriteAsync(peer, cancellationToken);

    public async ValueTask ReceiveBlockAsync(Block block, CancellationToken cancellationToken)
    {
        await _receiveBlocksChannel.Writer.WriteOrDisposeAsync(block, cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        _receiveBlocksChannel.Writer.TryComplete();
        _scheduleChannel.Writer.TryComplete();
    }
}
