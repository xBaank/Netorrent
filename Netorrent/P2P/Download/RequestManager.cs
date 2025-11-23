using System.Collections.Concurrent;
using System.Net;
using System.Threading.Channels;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Messages;
using ZLinq;

namespace Netorrent.P2P.Download;

internal class RequestManager(
    IReadOnlyDictionary<IPEndPoint, PeerConnection> activePeers,
    Bitfield myBitfield,
    FileManager fileManager
) : IAsyncDisposable
{
    const int MinPeersForRarity = 6;
    const int WarmupTimeoutSecods = 8;

    private readonly Channel<Block> _receiveBlocks = Channel.CreateBounded<Block>(
        new BoundedChannelOptions(256) { SingleWriter = true, SingleReader = true }
    );
    private readonly Channel<PeerConnection> _scheduleChannel =
        Channel.CreateBounded<PeerConnection>(
            new BoundedChannelOptions(50) { SingleReader = true, SingleWriter = false }
        );
    private readonly Lock _rarityLock = new();
    private readonly int[] _pieceRarity = new int[myBitfield.Length];
    private readonly ConcurrentDictionary<int, RequestBlock?[]> _requestBlocksByPieceIndex = [];
    private readonly ConcurrentDictionary<int, PieceBuffer> _pieceBuffers = [];
    private HashSet<int>? _currentRarestPieces;
    private Task? _requestManagerTask;

    public Task? WaitTask => _requestManagerTask;

    public void Start(CancellationToken cancellationToken) =>
        _requestManagerTask ??= RunRequestManagerAsync(cancellationToken);

    public async Task RunRequestManagerAsync(CancellationToken cancellationToken)
    {
        var faultedTask = await Task.WhenAny(
            ReceiveBlocksAsync(cancellationToken),
            ReScheduleTimedoutBlocksAsync(cancellationToken),
            SchedulePiecesAsync(cancellationToken)
        );
        await faultedTask;
    }

    public async Task ReceiveBlocksAsync(CancellationToken cancellationToken)
    {
        await foreach (var receiveBlock in _receiveBlocks.Reader.ReadAllAsync(cancellationToken))
        {
            if (!_pieceBuffers.TryGetValue(receiveBlock.Index, out var pieceBuffer))
            {
                pieceBuffer = new PieceBuffer(receiveBlock.Index, fileManager);
                _pieceBuffers[receiveBlock.Index] = pieceBuffer;
            }

            pieceBuffer.AddBlock(receiveBlock);

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
                        _currentRarestPieces?.Remove(receiveBlock.Index);
                        if (_currentRarestPieces?.Count == 0)
                        {
                            _currentRarestPieces = null;
                        }
                    }
                    _requestBlocksByPieceIndex.TryRemove(receiveBlock.Index, out _);
                    await myBitfield.SetPieceAsync(receiveBlock.Index, cancellationToken);
                }
            }
            finally
            {
                pieceBuffer.Dispose();
                _pieceBuffers.TryRemove(receiveBlock.Index, out _);
            }
        }
    }

    public async Task ReScheduleTimedoutBlocksAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            foreach (
                var requestBlock in _requestBlocksByPieceIndex
                    .Values.AsValueEnumerable()
                    .SelectMany(i => i)
                    .Where(i => i is not null)
            )
            {
                var passedTime =
                    DateTimeOffset.UtcNow - (requestBlock!.RequestedAt ?? DateTimeOffset.UtcNow);

                if (passedTime > 10.Seconds)
                {
                    requestBlock.State = RequestBlockState.Pending;
                    requestBlock.RequestedFrom = null;
                    requestBlock.RequestedAt = null;
                }
            }

            await Task.Delay(1.Seconds, cancellationToken);
        }
    }

    public async Task SchedulePiecesAsync(CancellationToken cancellationToken)
    {
        await WarmupAsync(cancellationToken);
        await foreach (
            var peerConnection in _scheduleChannel.Reader.ReadAllAsync(cancellationToken)
        )
        {
            lock (_rarityLock)
            {
                var toTake = _currentRarestPieces is null ? 250 : 250 - _currentRarestPieces.Count;

                var rarestPieces = _pieceRarity
                    .AsValueEnumerable()
                    .Select((rarity, index) => new { Index = index, Rarity = rarity })
                    .Where(i => !myBitfield.HasPiece(i.Index))
                    .OrderByDescending(i => i.Rarity)
                    .Take(500)
                    .Shuffle()
                    .Take(toTake)
                    .Select(i => i.Index)
                    .ToHashSet();

                _currentRarestPieces ??= rarestPieces;

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

    private async Task ScheduleRequests(
        PeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        var desired = peerConnection.DownloadSpeedTracker.CurrentBps.Kbps / 50;
        var max = Math.Clamp(desired, 8, 32);

        while (peerConnection.RequestedBlocksCount < max)
        {
            var block = SelectBlock(peerConnection);

            if (block is null)
                break;

            block.State = RequestBlockState.Requested;
            block.RequestedFrom = peerConnection;
            block.RequestedAt = DateTimeOffset.UtcNow;
            await peerConnection.AddRequestAsync(block, cancellationToken);
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
                    if (currentRequestBlock is null or { State: RequestBlockState.Pending })
                    {
                        return requestBlocks[i] = fileManager.GetRequestBlockByBlockIndex(index, i);
                    }
                }
            }
        }

        return null;
    }

    internal void IncreaseRarity(int index)
    {
        Interlocked.Increment(ref _pieceRarity[index]);
    }

    internal void DecreaseRarity(int index)
    {
        Interlocked.Decrement(ref _pieceRarity[index]);
    }

    internal async ValueTask OnPeerUnchockedAsync(
        PeerConnection peer,
        CancellationToken cancellationToken
    ) => await _scheduleChannel.Writer.WriteAsync(peer, cancellationToken);

    internal async ValueTask ReceiveBlockAsync(
        Block block,
        PeerConnection receiver,
        CancellationToken cancellationToken
    )
    {
        await _receiveBlocks.Writer.WriteAsync(block, cancellationToken);
        await _scheduleChannel.Writer.WriteAsync(receiver, cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        _receiveBlocks.Writer.TryComplete();
        _scheduleChannel.Writer.TryComplete();
        await foreach (var item in _receiveBlocks.Reader.ReadAllAsync())
        {
            item.Dispose();
        }
        foreach (var item in _pieceBuffers)
        {
            item.Value.Dispose();
        }
    }
}
