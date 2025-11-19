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
    private readonly Channel<RequestBlock> _requestBlocks = Channel.CreateBounded<RequestBlock>(
        new BoundedChannelOptions(fileManager.MaxBlocksByPiece * 5)
        {
            SingleWriter = false,
            SingleReader = false,
        }
    );
    private readonly Channel<Block> _receiveBlocks = Channel.CreateBounded<Block>(
        new BoundedChannelOptions(fileManager.MaxBlocksByPiece * 5)
        {
            SingleWriter = false,
            SingleReader = false,
        }
    );
    private readonly List<RequestBlock> _allRequestBlocks = fileManager.GetAllRequestBlocks();
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
        { }
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
                    .OrderByDescending(i => i.Rarity)
                    .ThenBy(_ => Random.Shared.Next())
                    .Take(8)
                    .ToArray();

                foreach (var requestBlock in blocksToRequest)
                {
                    await peerConnection.AddRequestAsync(requestBlock, cancellationToken);
                    requestBlock.State = RequestBlockState.Pending;
                    requestBlock.RequestedFrom = peerConnection;
                    requestBlock.RequestedAt = DateTimeOffset.UtcNow;
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

    internal async ValueTask ReceiveBlockAsync(Block block)
    {
        await _receiveBlocks.Writer.WriteAsync(block);
    }

    internal async ValueTask CancelRequestAsync(
        RequestBlock item,
        CancellationToken cancellationToken
    )
    {
        await _requestBlocks.Writer.WriteAsync(item, cancellationToken);
    }
}
