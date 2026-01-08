using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Netorrent.Extensions;
using Netorrent.P2P.Messages;
using ZLinq;

namespace Netorrent.P2P.Download;

internal class PiecePicker(Bitfield myBitfield, int blockSize, int pieceLenght, long totalSize)
    : IPiecePicker
{
    public const int TimeoutSeconds = 10;

    private readonly int[] _pieceRarity = new int[myBitfield.Length];
    private readonly Dictionary<int, RequestBlock[]> _requestBlocks = [];
    public int BlockSize => blockSize;

    public void IncreaseRarity(int index)
    {
        Interlocked.Increment(ref _pieceRarity[index]);
    }

    public void DecreaseRarity(int index)
    {
        Interlocked.Decrement(ref _pieceRarity[index]);
    }

    public void CompleteRequestBlock(RequestBlock requestBlock)
    {
        requestBlock.State = RequestBlockState.Completed;
        requestBlock.RequestedAt = null;
        requestBlock.RequestedFrom.Clear();
    }

    public void CompletePiece(int index)
    {
        _requestBlocks.Remove(index);
    }

    public bool TryGetRequestedBlock(
        Block receiveBlock,
        [NotNullWhen(true)] out RequestBlock? requestBlock
    )
    {
        if (_requestBlocks.TryGetValue(receiveBlock.Index, out var requestBlocks))
        {
            foreach (var requestedBlock in requestBlocks)
            {
                if (
                    requestedBlock.State != RequestBlockState.Completed
                    && requestedBlock.Index == receiveBlock.Index
                    && requestedBlock.Begin == receiveBlock.Begin
                    && requestedBlock.Length == receiveBlock.Payload.Length
                    && requestedBlock.RequestedFrom.Contains(receiveBlock.FromPeer)
                )
                {
                    requestBlock = requestedBlock;
                    return true;
                }
            }
        }

        requestBlock = null;
        return false;
    }

    public RequestBlock? GetBlock(Bitfield bitfield)
    {
        HashSet<int> excludedIndices = [];

        foreach (var requestBlock in _requestBlocks.Values.AsValueEnumerable().SelectMany(i => i))
        {
            excludedIndices.Add(requestBlock.Index);

            if (
                requestBlock.State == RequestBlockState.Pending
                && bitfield.HasPiece(requestBlock.Index)
            )
                return requestBlock;
        }

        var piece = GetPiece(bitfield, excludedIndices);

        if (piece is null)
            return null;

        var blockCount = GetBlockCountByPieceIndex(piece.Value);

        if (!_requestBlocks.TryGetValue(piece.Value, out var requestBlocks))
        {
            requestBlocks = new RequestBlock[blockCount];
            _requestBlocks[piece.Value] = requestBlocks;

            for (int i = 0; i < blockCount; i++)
            {
                requestBlocks[i] = GetRequestBlockByBlockIndex(piece.Value, i);
            }
        }

        return requestBlocks[0];
    }

    public IEnumerable<RequestBlock> GetTimeoutRequestBlocks()
    {
        var timeout = TimeoutSeconds.Seconds;
        var now = DateTime.UtcNow;

        return _requestBlocks
            .Values.SelectMany(i => i)
            .Where(i =>
                i?.State == RequestBlockState.Requested
                && i.RequestedAt is not null
                && (now - i.RequestedAt.Value) > timeout
            )
            .Cast<RequestBlock>();
    }

    public IPeerConnection? GetLastRequesterOrNull(RequestBlock requestBlock)
    {
        return requestBlock.RequestedFrom.Count == 0 ? null : requestBlock.RequestedFrom[^1];
    }

    public void SetBlockToPending(RequestBlock requestBlock)
    {
        requestBlock.State = RequestBlockState.Pending;
        requestBlock.RequestedAt = null;
    }

    public void SetBlockToRequested(RequestBlock requestBlock, IPeerConnection peerConnection)
    {
        requestBlock.State = RequestBlockState.Requested;
        requestBlock.RequestedFrom.Add(peerConnection);
        requestBlock.RequestedAt = DateTimeOffset.UtcNow;
    }

    private int? GetPiece(Bitfield peerBitfield, HashSet<int> excluded)
    {
        var posiblePieces = new List<int>(myBitfield.Length);
        for (int i = 0; i < peerBitfield.Length; i++)
        {
            if (peerBitfield.HasPiece(i) && !myBitfield.HasPiece(i) && !excluded.Contains(i))
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

    public int GetBlockCountByPieceIndex(int pieceIndex)
    {
        var pieceSize = GetPieceSize(pieceIndex);
        int blockCount = (pieceSize + BlockSize - 1) / BlockSize;
        return blockCount;
    }

    public int GetPieceSize(int pieceIndex)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(pieceIndex);

        var pieceCount = (totalSize + pieceLenght - 1) / pieceLenght;
        if (pieceIndex >= pieceCount)
            throw new ArgumentOutOfRangeException(nameof(pieceIndex));

        var pieceLength =
            (pieceIndex == pieceCount - 1)
                ? totalSize - (long)pieceIndex * pieceLenght
                : pieceLenght;

        return (int)pieceLength;
    }

    public RequestBlock GetRequestBlockByBlockIndex(int pieceIndex, int blockIndex)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(pieceIndex);
        ArgumentOutOfRangeException.ThrowIfNegative(blockIndex);
        var pieceCount = (totalSize + pieceLenght - 1) / pieceLenght;
        if (pieceIndex >= pieceCount)
            throw new ArgumentOutOfRangeException(nameof(pieceIndex));
        var pieceLength =
            (pieceIndex == pieceCount - 1)
                ? totalSize - (long)pieceIndex * pieceLenght
                : pieceLenght;
        int blockCount = (int)((pieceLength + BlockSize - 1) / BlockSize);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(blockIndex, blockCount);
        int begin = blockIndex * BlockSize;
        int length = (int)Math.Min(BlockSize, pieceLength - begin);
        return new RequestBlock(pieceIndex, begin, length);
    }

    public async ValueTask DisposeAsync() { }
}
