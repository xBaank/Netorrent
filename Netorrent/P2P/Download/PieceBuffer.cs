using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Messages;
using ZLinq;

namespace Netorrent.P2P.Download;

internal class PieceBuffer : IDisposable
{
    private readonly List<Block> _buffer;
    private readonly bool[] _blockReceivedFlags;
    private readonly int _blocksCount;
    private readonly int _index;
    private readonly FileManager _fileManager;

    public PieceBuffer(int index, FileManager fileManager)
    {
        _index = index;
        _fileManager = fileManager;
        _blocksCount = fileManager.GetBlockCountByPieceIndex(index);
        _blockReceivedFlags = new bool[_blocksCount];
        _buffer = new List<Block>(_blocksCount);
    }

    public void AddBlock(Block block)
    {
        var blockIndex = block.BlockIndex;
        if (_blockReceivedFlags[blockIndex])
        {
            block.Dispose();
            return;
        }
        _buffer.Add(block);
        _blockReceivedFlags[blockIndex] = true;
    }

    public bool IsComplete => _buffer.Count == _blocksCount;

    public async ValueTask<bool> WritePieceAsync(CancellationToken cancellationToken)
    {
        if (!IsComplete)
        {
            return false;
        }

        using var data = _buffer
            .AsValueEnumerable()
            .OrderBy(i => i.Begin)
            .Select(i => i.Payload.Memory)
            .ToArray()
            .Combine();

        var isOK = await _fileManager.VerifyPieceAsync(_index, data.Memory, cancellationToken);

        if (isOK)
        {
            await _fileManager.WritePieceAsync(_index, 0, data.Memory, cancellationToken);
        }

        return isOK;
    }

    public void Dispose()
    {
        foreach (var item in _buffer)
        {
            item.Dispose();
        }
    }
}
