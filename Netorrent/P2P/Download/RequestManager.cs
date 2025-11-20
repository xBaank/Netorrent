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
)
{
    private readonly Channel<Block> _receiveBlocks = Channel.CreateBounded<Block>(
        new BoundedChannelOptions(fileManager.MaxBlocksByPiece * 5)
        {
            SingleWriter = false,
            SingleReader = false,
        }
    );
    private readonly List<RequestBlock> _allRequestBlocks = fileManager.GetAllRequestBlocks();
    private readonly ConcurrentDictionary<int, PieceBuffer> _pieceBuffers = [];
    private Task? _requestManagerTask;

    public Task? WaitTask => _requestManagerTask;

    public void Start(CancellationToken cancellationToken) =>
        _requestManagerTask ??= RunRequestManagerAsync(cancellationToken);

    public async Task RunRequestManagerAsync(CancellationToken cancellationToken)
    {
        var faultedTask = await Task.WhenAny(
            ReceiveBlocksAsync(cancellationToken),
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

    public async Task ScheduleBlocksAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            foreach (
                var peerConnection in activePeers
                    .Values.AsValueEnumerable()
                    .Where(i => i.AmInterested && i.RequestedBlocksCount < 8)
            )
            {
                var blocksToRequest = _allRequestBlocks
                    .AsValueEnumerable()
                    .Where(i => i.State == RequestBlockState.Pending)
                    .Where(i => peerConnection.PeerBitField.HasPiece(i.Index))
                    .OrderByDescending(i => i.Rarity)
                    .Take(fileManager.MaxBlocksByPiece * 5)
                    .OrderByDescending(_ => Random.Shared.Next())
                    .Take(8 - peerConnection.RequestedBlocksCount)
                    .ToArray();

                foreach (var requestBlock in blocksToRequest)
                {
                    requestBlock.State = RequestBlockState.Requested;
                    requestBlock.RequestedFrom = peerConnection;
                    requestBlock.RequestedAt = DateTimeOffset.UtcNow;
                    await peerConnection.AddRequestAsync(requestBlock, cancellationToken);
                }
            }

            await Task.Delay(500, cancellationToken);
        }
    }

    internal void IncreaseRarity(int index)
    {
        foreach (var item in _allRequestBlocks.AsValueEnumerable().Where(i => i.Index == index))
        {
            item.Rarity++;
        }
    }

    internal void DecreaseRarity(int index)
    {
        foreach (var item in _allRequestBlocks.AsValueEnumerable().Where(i => i.Index == index))
        {
            item.Rarity--;
        }
    }

    internal async ValueTask ReceiveBlockAsync(Block block, CancellationToken cancellationToken)
    {
        var requestBlock = _allRequestBlocks
            .AsValueEnumerable()
            .FirstOrDefault(i => i.Index == block.Index && i.Begin == block.Begin);

        if (requestBlock is not null)
        {
            requestBlock.State = RequestBlockState.Received;
            requestBlock.RequestedFrom = null;
            requestBlock.RequestedAt = null;
        }

        await _receiveBlocks.Writer.WriteAsync(block, cancellationToken);
    }
}
