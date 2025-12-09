using System.Collections.Concurrent;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Messages;
using ZLinq;

namespace Netorrent.P2P.Download;

internal class PiecePicker(Bitfield myBitfield, FileManager fileManager) : IAsyncDisposable
{
    public const int TimeoutSeconds = 10;

    private readonly int[] _pieceRarity = new int[myBitfield.Length];
    public readonly ConcurrentDictionary<int, RequestBlock?[]> _requestBlocksByPieceIndex = [];
    private readonly ConcurrentDictionary<int, PieceBuffer> _pieceBuffers = [];

    public void IncreaseRarity(int index)
    {
        Interlocked.Increment(ref _pieceRarity[index]);
    }

    public void DecreaseRarity(int index)
    {
        Interlocked.Decrement(ref _pieceRarity[index]);
    }

    public async ValueTask ReceiveBlockAsync(
        Block receiveBlock,
        CancellationToken cancellationToken
    )
    {
        if (!_pieceBuffers.TryGetValue(receiveBlock.Index, out var pieceBuffer))
        {
            pieceBuffer = new PieceBuffer(receiveBlock.Index, fileManager);
            _pieceBuffers[receiveBlock.Index] = pieceBuffer;
        }

        pieceBuffer.AddBlock(receiveBlock);

        if (_requestBlocksByPieceIndex.TryGetValue(receiveBlock.Index, out var requestBlocks))
        {
            var requestBlock = requestBlocks
                .AsValueEnumerable()
                .FirstOrDefault(i => i?.Begin == receiveBlock.Begin);

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

                //Decrement from all the peers that requested
                foreach (var peer in requestBlock.RequestedFrom)
                {
                    peer.DecrementRequestedBlock();
                }
            }

            requestBlock?.State = RequestBlockState.Completed;
            requestBlock?.RequestedAt = null;
            requestBlock?.RequestedFrom.Clear();
        }

        if (!pieceBuffer.IsComplete)
            return;

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
                myBitfield.SetPiece(receiveBlock.Index);
                _requestBlocksByPieceIndex.TryRemove(receiveBlock.Index, out _);
            }
        }
        finally
        {
            pieceBuffer.Dispose();
            _pieceBuffers.TryRemove(receiveBlock.Index, out _);
        }
    }

    public RequestBlock? SelectBlock(int index)
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

            if (currentRequestBlock?.State == RequestBlockState.Completed)
            {
                continue;
            }

            if (currentRequestBlock is null or { State: RequestBlockState.Pending })
            {
                return requestBlocks[i] ??= fileManager.GetRequestBlockByBlockIndex(index, i);
            }
        }

        return null;
    }

    public (RequestBlock requestBlock, TimeSpan passedTime)[] GetTimeoutRequestBlocks()
    {
        var timeout = TimeoutSeconds.Seconds;
        var now = DateTime.UtcNow;

        return _requestBlocksByPieceIndex
            .Values.AsValueEnumerable()
            .SelectMany(i => i)
            .Where(i =>
                i?.State == RequestBlockState.Requested
                && i.RequestedAt is not null
                && (now - i.RequestedAt.Value) > timeout
            )
            .Cast<RequestBlock>()
            .Select(i => (i, (now - i.RequestedAt!.Value)))
            .ToArray();
    }

    private IEnumerable<int> GetPriorityPieces(Bitfield peerBitfield)
    {
        foreach (var (pieceIndex, blocks) in _requestBlocksByPieceIndex)
        {
            // peer must have it + we don't already own it
            if (!peerBitfield.HasPiece(pieceIndex) || myBitfield.HasPiece(pieceIndex))
                continue;

            // only return pieces that have pending/incomplete blocks
            if (
                blocks
                    .AsValueEnumerable()
                    .Any(b => b is null or { State: RequestBlockState.Pending })
            )
                yield return pieceIndex;
        }
    }

    public int? GetPiece(Bitfield peerBitfield)
    {
        //Try to get pieces with priority (already started)
        var priorityPieces = GetPriorityPieces(peerBitfield).AsValueEnumerable().ToArray();
        if (priorityPieces.Length > 0)
        {
            // cheap shuffle/random pick
            return priorityPieces.AsValueEnumerable().Shuffle().First();
        }

        //Fallback to rarest
        var posiblePieces = new List<int>(myBitfield.Length);
        for (int i = 0; i < peerBitfield.Length; i++)
        {
            if (
                peerBitfield.HasPiece(i)
                && !myBitfield.HasPiece(i)
                && !_requestBlocksByPieceIndex.ContainsKey(i) //If we didn't return any of these then we exclude them
            )
            {
                posiblePieces.Add(i);
            }
        }

        if (posiblePieces.Count == 0)
            return null;

        (int index, int rarity)[] posiblePiecesWithRarity = posiblePieces
            .AsValueEnumerable()
            .Select(index => (index, _pieceRarity[index]))
            .OrderBy(i => i.Item2)
            .ToArray();

        //Get 10% or the first 500 of rarest -> shuffle -> take first
        var rarestCount = Math.Min(posiblePiecesWithRarity.Length / 10, 500);
        rarestCount = rarestCount < 1 ? 1 : rarestCount;

        var selectedPiece = posiblePiecesWithRarity
            .AsValueEnumerable()
            .Take(rarestCount)
            .Shuffle()
            .First();

        return selectedPiece.index;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var item in _pieceBuffers)
        {
            item.Value.Dispose();
        }
    }
}
