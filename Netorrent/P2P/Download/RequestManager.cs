using System.Collections.Concurrent;
using System.Collections.Immutable;
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
    private readonly Channel<Block> _receiveBlocks = Channel.CreateBounded<Block>(
        new BoundedChannelOptions(fileManager.MaxBlocksByPiece * 5)
        {
            SingleWriter = true,
            SingleReader = true,
        }
    );
    private readonly ImmutableArray<RequestBlock> _allRequestBlocks = fileManager
        .GetAllRequestBlocks()
        .ToImmutableArray();
    private readonly ConcurrentDictionary<int, PieceBuffer> _pieceBuffers = [];
    private Task? _requestManagerTask;

    public Task? WaitTask => _requestManagerTask;

    public void Start(CancellationToken cancellationToken) =>
        _requestManagerTask ??= RunRequestManagerAsync(cancellationToken);

    public async Task RunRequestManagerAsync(CancellationToken cancellationToken)
    {
        var faultedTask = await Task.WhenAny(
            ReceiveBlocksAsync(cancellationToken),
            ReScheduleTimedoutBlocksAsync(cancellationToken),
            ScheduleBlocksAsync(cancellationToken)
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
                }
                else
                {
                    await myBitfield.SetPieceAsync(receiveBlock.Index, cancellationToken);
                }
            }
            finally
            {
                pieceBuffer.Dispose();
            }
        }
    }

    public async Task ReScheduleTimedoutBlocksAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var timedOutRequestBlocks = _allRequestBlocks
                .AsValueEnumerable()
                .Where(i => i.State == RequestBlockState.Requested)
                .Where(i => i.RequestedAt.HasValue)
                .Where(i => DateTimeOffset.UtcNow - i.RequestedAt!.Value > 10.Seconds);

            foreach (var requestBlock in timedOutRequestBlocks)
            {
                requestBlock.State = RequestBlockState.Pending;
                requestBlock.RequestedAt = null;
            }

            await Task.Delay(1.Seconds, cancellationToken);
        }
    }

    public async Task ScheduleBlocksAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            foreach (
                var peerConnection in activePeers
                    .Values.AsValueEnumerable()
                    .Where(i => i.AmInterested)
                    .OrderByDescending(i => i.DownloadSpeedTracker.CurrentBps.Bps)
                    .ToArray()
            )
            {
                var max = peerConnection.DownloadSpeedTracker.CurrentBps.Kbps switch
                {
                    >= 1000 => 32,
                    >= 500 => 16,
                    >= 200 => 12,
                    >= 50 => 10,
                    _ => 8,
                };

                if (max < peerConnection.RequestedBlocksCount)
                    continue;

                var blocksToRequest = _allRequestBlocks
                    .AsValueEnumerable()
                    .Where(i => i.State == RequestBlockState.Pending)
                    .Where(i => peerConnection.PeerBitField.HasPiece(i.Index))
                    .OrderByDescending(i => i.PieceRarity?.Rarity ?? 0)
                    .Take(fileManager.MaxBlocksByPiece * 5)
                    .Shuffle()
                    .Take(max - peerConnection.RequestedBlocksCount)
                    .ToArray();

                foreach (var requestBlock in blocksToRequest)
                {
                    requestBlock.State = RequestBlockState.Requested;
                    requestBlock.RequestedFrom.Add(peerConnection);
                    requestBlock.RequestedAt = DateTimeOffset.UtcNow;
                    await peerConnection.AddRequestAsync(requestBlock, cancellationToken);
                }
            }

            await Task.Delay(250.Milliseconds, cancellationToken);
        }
    }

    internal void IncreaseRarity(int index)
    {
        var pieceRarity = _allRequestBlocks
            .AsValueEnumerable()
            .FirstOrDefault(i => i.Index == index)
            ?.PieceRarity;

        if (pieceRarity is not null)
            pieceRarity.Rarity++;
    }

    internal void DecreaseRarity(int index)
    {
        var pieceRarity = _allRequestBlocks
            .AsValueEnumerable()
            .FirstOrDefault(i => i.Index == index)
            ?.PieceRarity;

        if (pieceRarity is not null)
            pieceRarity.Rarity--;
    }

    internal async ValueTask ReceiveBlockAsync(
        Block block,
        PeerConnection receiver,
        CancellationToken cancellationToken
    )
    {
        var requestBlock = _allRequestBlocks
            .AsValueEnumerable()
            .FirstOrDefault(i => i.Index == block.Index && i.Begin == block.Begin);

        if (requestBlock is null)
        {
            block.Dispose();
            return;
        }

        if (requestBlock.State != RequestBlockState.Requested)
        {
            block.Dispose();
            return;
        }

        var cancelTasks = requestBlock
            .RequestedFrom.AsValueEnumerable()
            .Where(i => i != receiver)
            .Select(i => i.SendCancelAsync(requestBlock, cancellationToken))
            .ToArray();

        await Task.WhenAll(cancelTasks);

        requestBlock.State = RequestBlockState.Received;
        requestBlock.RequestedFrom = [];
        requestBlock.RequestedAt = null;
        await _receiveBlocks.Writer.WriteAsync(block, cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        _receiveBlocks.Writer.TryComplete();
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
