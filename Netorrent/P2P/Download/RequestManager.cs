using System.Collections.Concurrent;
using System.Net;
using System.Threading;
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
    private readonly ConcurrentDictionary<int, int> _pieceBlockCount = [];
    private readonly ConcurrentDictionary<
        int,
        (Lock @lock, List<RequestBlock> requestBlocks)
    > _pendingRequestBlocksByPiece = [];
    private readonly ConcurrentDictionary<int, List<Block>> _blocksByPiece = [];
    private Task? _requestManagerTask;

    public Task? WaitTask => _requestManagerTask;

    public void Start(CancellationToken cancellationToken) =>
        _requestManagerTask ??= RunRequestManagerAsync(cancellationToken);

    public async Task RunRequestManagerAsync(CancellationToken cancellationToken)
    {
        var faultedTask = await Task.WhenAny(
            ReceiveBlocksAsync(cancellationToken),
            RequestBlocksAsync(cancellationToken),
            ScheduleBlocksAsync(cancellationToken)
        );
        await faultedTask;
    }

    public async Task ReceiveBlocksAsync(CancellationToken cancellationToken)
    {
        await foreach (var receiveBlock in _receiveBlocks.Reader.ReadAllAsync(cancellationToken))
        {
            if (
                _pendingRequestBlocksByPiece.TryGetValue(
                    receiveBlock.Index,
                    out var _pendingRequestBlocks
                )
            )
            {
                lock (_pendingRequestBlocks.@lock)
                {
                    _pendingRequestBlocks.requestBlocks.RemoveAll(i =>
                        i.Index == receiveBlock.Index && i.Begin == receiveBlock.Begin
                    );
                }
            }

            var blocks = _blocksByPiece[receiveBlock.Index];
            blocks.Add(receiveBlock);

            if (
                _pieceBlockCount.TryGetValue(receiveBlock.Index, out var blocksCount)
                && blocksCount == blocks.Count
            )
            {
                try
                {
                    using var combined = blocks
                        .AsValueEnumerable()
                        .OrderBy(i => i.Begin)
                        .Select(i => i.Payload.Memory)
                        .ToArray()
                        .Combine();

                    var begin = blocks.AsValueEnumerable().Min(i => i.Begin);
                    var isOk = await fileManager.VerifyPieceAsync(
                        receiveBlock.Index,
                        combined.Memory,
                        cancellationToken
                    );

                    if (isOk)
                    {
                        await fileManager.WritePieceAsync(
                            pieceIndex: receiveBlock.Index,
                            begin,
                            combined.Memory,
                            cancellationToken
                        );

                        _pendingRequestBlocksByPiece.TryRemove(receiveBlock.Index, out _);
                        await myBitfield.SetPieceAsync(receiveBlock.Index, cancellationToken);
                    }
                }
                finally
                {
                    foreach (var item in blocks)
                    {
                        item.Dispose();
                    }
                }
            }
        }
    }

    public async Task RequestBlocksAsync(CancellationToken cancellationToken)
    {
        List<PeerConnection> availablePeers = [];
        await foreach (var requestBlock in _requestBlocks.Reader.ReadAllAsync(cancellationToken))
        {
            if (availablePeers.Count == 0)
            {
                availablePeers = activePeers
                    .Values.AsValueEnumerable()
                    .Where(i => i.AmInterested && !i.PeerChocking)
                    .Where(i => i.RequestedBlocksCount <= 8)
                    .Where(i =>
                        _pendingRequestBlocksByPiece.Keys.Any(x => i.PeerBitField.HasPiece(x))
                    )
                    .ToList();
            }

            var next = Random.Shared.Next(availablePeers.Count);
            var peer = availablePeers[next];

            if (!peer.PeerChocking && peer.AmInterested)
                await peer.AddRequestAsync(requestBlock);
            else
            {
                await _requestBlocks.Writer.WriteAsync(requestBlock, cancellationToken);
                availablePeers.Remove(peer);
            }

            if (peer.RequestedBlocksCount >= 8)
            {
                availablePeers.Remove(peer);
            }
        }
    }

    public async Task ScheduleBlocksAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var peersWithMissingPieces = activePeers
                .Values.AsValueEnumerable()
                .Where(i => myBitfield.HasAnyMissingPiece(i.PeerBitField))
                .ToArray();

            var allBitfields = peersWithMissingPieces
                .AsValueEnumerable()
                .Select(i => i.PeerBitField);

            var rarityByPieces = new Dictionary<int, int>();

            for (var i = 0; i < myBitfield.Length; i++)
            {
                rarityByPieces[i] = 0;
            }

            foreach (var pieces in allBitfields.Select(i => i.Pieces))
            {
                for (int i = 0; i < pieces.Count; i++)
                {
                    if (pieces[i])
                        rarityByPieces[i]++;
                }
            }

            //Take 5 pieces among the 10 rarest pieces
            var mostRarePieces = rarityByPieces
                .AsValueEnumerable()
                .OrderBy(i => i.Value)
                .Where(i =>
                    !myBitfield.HasPiece(i.Key) && !_pendingRequestBlocksByPiece.ContainsKey(i.Key)
                )
                .Take(10)
                .OrderBy(_ => Random.Shared.Next())
                .Take(5)
                .ToArray();

            //Schedule requests for the rare pieces
            foreach (var (rarePiece, _) in mostRarePieces)
            {
                var requestBlocks = fileManager.GetBlocksByPieceIndex(rarePiece);
                _pieceBlockCount[rarePiece] = requestBlocks.Length;
                var @lock = new Lock();
                _pendingRequestBlocksByPiece[rarePiece] = (@lock, []);
                foreach (var requestBlock in requestBlocks)
                {
                    await _requestBlocks.Writer.WriteAsync(requestBlock, cancellationToken);
                    lock (@lock)
                    {
                        _pendingRequestBlocksByPiece[rarePiece].requestBlocks.Add(requestBlock);
                    }
                }
            }
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
