using System.Security.Cryptography;
using Netorrent.IO;
using Netorrent.P2P.Messages;
using ZLinq;

namespace Netorrent.P2P.Download;

internal class PieceBuffer : IDisposable
{
    private readonly IncrementalHash _incrementalHash = IncrementalHash.CreateHash(
        HashAlgorithmName.SHA1
    );
    private readonly bool[] _blockReceivedFlags; //TODO use bitarray or bitmask (long)?
    private readonly int _blocksCount;
    private readonly int _index;
    private readonly IPieceStorage _pieceWriter;
    private readonly int _blockSize;
    private readonly Dictionary<int, Block> _pendingBlocks = [];
    private int _nextExpectedBlockIndex = 0;

    public int Size { get; }

    public bool IsComplete => _blockReceivedFlags.AsValueEnumerable().All(x => x);

    public PieceBuffer(int index, IPieceStorage pieceWriter, IPiecePicker piecePicker)
    {
        _index = index;
        _pieceWriter = pieceWriter;
        _blockSize = piecePicker.BlockSize;
        _blocksCount = piecePicker.GetBlockCountByPieceIndex(index);
        var pieceSize = piecePicker.GetPieceSize(index);
        _blockReceivedFlags = new bool[_blocksCount];
        Size = pieceSize;
    }

    public async ValueTask AddBlockAsync(Block block, CancellationToken ct)
    {
        var blockIndex = block.Begin / _blockSize;

        if (_blockReceivedFlags[blockIndex])
        {
            block.Dispose();
            return;
        }

        _pendingBlocks[blockIndex] = block;
        _blockReceivedFlags[blockIndex] = true;

        await FlushSequentialBlocksAsync(ct).ConfigureAwait(false);
    }

    private async ValueTask FlushSequentialBlocksAsync(CancellationToken ct)
    {
        while (_pendingBlocks.TryGetValue(_nextExpectedBlockIndex, out var block))
        {
            using (block)
            {
                var memory = block.Payload.Memory;

                _incrementalHash.AppendData(memory.Span);

                await _pieceWriter
                    .WriteAsync(_index, _nextExpectedBlockIndex * _blockSize, memory, ct)
                    .ConfigureAwait(false);

                _pendingBlocks.Remove(_nextExpectedBlockIndex);

                _nextExpectedBlockIndex++;
            }
        }
    }

    public bool VerifyPiece()
    {
        if (!IsComplete)
        {
            return false;
        }

        Span<byte> hash = stackalloc byte[20];

        if (_incrementalHash.TryGetHashAndReset(hash, out _))
        {
            ReadOnlySpan<byte> readHash = hash;
            return _pieceWriter.VerifyPieceHash(_index, ref readHash);
        }

        return false;
    }

    public void Dispose()
    {
        foreach (var item in _pendingBlocks)
        {
            item.Value.Dispose();
        }
        _incrementalHash.Dispose();
    }
}
