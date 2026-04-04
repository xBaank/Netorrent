using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using Netorrent.P2P.Messages;
using ZLinq;

namespace Netorrent.P2P.Download;

internal class PiecePicker(Bitfield myBitfield, int blockSize, int pieceLenght, long totalSize)
    : IPiecePicker
{
    private readonly int[] _pieceRarity = new int[myBitfield.Length];
    private readonly Dictionary<int, RequestBlock[]> _requestBlocks = [];
    private readonly HashSet<int> _requestedIndexes = [];
    private bool _isEndGame = false;
    private int _unrequestedPieceCount = CountUnrequestedPieces(myBitfield);
    private int _pendingBlockCount;
    public int BlockSize => blockSize;
    public bool IsEndGame => _isEndGame;

    private static int CountUnrequestedPieces(Bitfield bitfield)
    {
        var count = 0;
        for (int i = 0; i < bitfield.Length; i++)
            if (!bitfield.HasPiece(i))
                count++;
        return count;
    }

    public void IncreaseRarity(int index)
    {
        Interlocked.Increment(ref _pieceRarity[index]);
    }

    public void DecreaseRarity(int index)
    {
        Interlocked.Decrement(ref _pieceRarity[index]);
    }

    public void CompletePiece(int index)
    {
        if (_requestedIndexes.Remove(index))
        {
            _unrequestedPieceCount++; // back to unrequested until ConfirmPiece is called
            _isEndGame = _unrequestedPieceCount == 0;
        }
        _requestBlocks.Remove(index);
    }

    public void ConfirmPiece(int index)
    {
        _unrequestedPieceCount--;
        _isEndGame = _unrequestedPieceCount == 0;
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

    public bool TryGetRequestBlock(
        IPeerConnection peerConnection,
        [NotNullWhen(true)] out RequestBlock? requestBlock
    )
    {
        if (peerConnection.PeerBitField is null)
        {
            requestBlock = null;
            return false;
        }

        if (_isEndGame || _pendingBlockCount > 0)
        {
            foreach (var item in _requestBlocks.Values.AsValueEnumerable().SelectMany(i => i))
            {
                if (
                    (_isEndGame || item.State == RequestBlockState.Pending)
                    && peerConnection.PeerBitField.HasPiece(item.Index)
                    && !item.RequestedFrom.Contains(peerConnection)
                )
                {
                    if (item.State == RequestBlockState.Pending)
                        _pendingBlockCount--;
                    requestBlock = item;
                    return true;
                }
            }
        }

        var piece = GetPiece(peerConnection.PeerBitField, _requestedIndexes);

        if (piece is null)
        {
            requestBlock = null;
            return false;
        }

        var blockCount = GetBlockCountByPieceIndex(piece.Value);

        if (!_requestBlocks.TryGetValue(piece.Value, out var requestBlocks))
        {
            requestBlocks = new RequestBlock[blockCount];
            _requestBlocks[piece.Value] = requestBlocks;

            for (int i = 0; i < blockCount; i++)
            {
                requestBlocks[i] = GetRequestBlockByBlockIndex(piece.Value, i);
            }

            // All blocks start as Pending; first one will be returned below
            _pendingBlockCount += blockCount - 1;
        }

        _requestedIndexes.Add(piece.Value);
        _unrequestedPieceCount--;
        _isEndGame = _unrequestedPieceCount == 0;

        requestBlock = requestBlocks[0];
        return true;
    }

    public IEnumerable<RequestBlock> GetTimeoutRequestBlocks()
    {
        var now = DateTime.UtcNow;

        return _requestBlocks
            .Values.SelectMany(i => i)
            .Where(i =>
                i.State == RequestBlockState.Requested
                && i.TimeoutAt is not null
                && now > i.TimeoutAt.Value
            );
    }

    public void ResetBlocksToPending(IPeerConnection peer)
    {
        foreach (var blocks in _requestBlocks.Values)
        {
            foreach (var block in blocks)
            {
                if (
                    block.State == RequestBlockState.Requested
                    && block.RequestedFrom.Contains(peer)
                )
                {
                    block.State = RequestBlockState.Pending;
                    block.TimeoutAt = null;
                    _pendingBlockCount++;
                }
            }
        }
    }

    private int? GetPiece(Bitfield peerBitfield, HashSet<int> excluded)
    {
        var buffer = ArrayPool<int>.Shared.Rent(peerBitfield.Length);
        try
        {
            int count = 0;
            for (int i = 0; i < peerBitfield.Length; i++)
            {
                if (peerBitfield.HasPiece(i) && !myBitfield.HasPiece(i) && !excluded.Contains(i))
                    buffer[count++] = i;
            }

            if (count == 0)
                return null;

            // Sort only the filled portion by rarity ascending — no LINQ allocation
            var span = buffer.AsSpan(0, count);
            span.Sort((a, b) => _pieceRarity[a].CompareTo(_pieceRarity[b]));

            // Take top 10% (min 1, max 500), shuffle in-place, pick first
            var rarestCount = Math.Max(1, Math.Min(count / 10, 500));
            ShuffleSpan(span[..rarestCount]);
            return span[0];
        }
        finally
        {
            ArrayPool<int>.Shared.Return(buffer);
        }
    }

    private static void ShuffleSpan(Span<int> span)
    {
        for (int i = span.Length - 1; i > 0; i--)
        {
            int j = Random.Shared.Next(i + 1);
            (span[i], span[j]) = (span[j], span[i]);
        }
    }

    public int GetBlockCountByPieceIndex(int pieceIndex)
    {
        var pieceSize = GetPieceSize(pieceIndex);
        int blockCount = (pieceSize + BlockSize - 1) / BlockSize;
        return blockCount;
    }

    //TODO Move this to bitfield class?

    public int GetPieceSize(int pieceIndex)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(pieceIndex);

        var pieceCount = (totalSize + pieceLenght - 1) / pieceLenght;
        if (pieceIndex >= pieceCount)
        {
            throw new ArgumentOutOfRangeException(nameof(pieceIndex));
        }

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
        {
            throw new ArgumentOutOfRangeException(nameof(pieceIndex));
        }

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

    public long GetBitfieldSize()
    {
        var total = 0L;
        for (var i = 0; i < myBitfield.Length; i++)
        {
            if (myBitfield.HasPiece(i))
            {
                total += GetPieceSize(i);
            }
        }
        return total;
    }
}
