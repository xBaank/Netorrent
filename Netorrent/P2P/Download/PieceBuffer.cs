using System.Buffers;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Messages;
using ZLinq;

namespace Netorrent.P2P.Download;

internal class PieceBuffer : IDisposable
{
    private readonly RentedArray<byte> _buffer;
    private readonly bool[] _blockReceivedFlags; //TODO use bitarray or bitmask (long)?
    private readonly int _blocksCount;
    private readonly int _index;
    private readonly FileManager _fileManager;

    public int Size { get; }

    public PieceBuffer(int index, FileManager fileManager)
    {
        _index = index;
        _fileManager = fileManager;
        _blocksCount = fileManager.GetBlockCountByPieceIndex(index);
        var pieceSize = fileManager.GetPieceSize(index);
        _buffer = new RentedArray<byte>(ArrayPool<byte>.Shared.Rent(pieceSize), pieceSize);
        _blockReceivedFlags = new bool[_blocksCount];
        Size = pieceSize;
    }

    public void AddBlock(Block block)
    {
        var blockIndex = block.BlockIndex;
        if (_blockReceivedFlags[blockIndex])
        {
            block.Dispose();
            return;
        }
        block.Payload.Memory.CopyTo(_buffer.Memory[block.Begin..]);
        _blockReceivedFlags[blockIndex] = true;
    }

    public bool IsComplete => _blockReceivedFlags.AsValueEnumerable().All(x => x);

    public async ValueTask<bool> WritePieceAsync(CancellationToken cancellationToken)
    {
        if (!IsComplete)
        {
            return false;
        }

        var isOK = await _fileManager
            .VerifyPieceAsync(_index, _buffer.Memory, cancellationToken)
            .ConfigureAwait(false);

        if (isOK)
        {
            await _fileManager
                .WriteAsync(_index, 0, _buffer.Memory, cancellationToken)
                .ConfigureAwait(false);
        }

        return isOK;
    }

    public void Dispose()
    {
        _buffer.Dispose();
    }
}
