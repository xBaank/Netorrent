using System.Collections.Concurrent;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.Other;
using Netorrent.P2P.Messages;
using ZLinq;

namespace Netorrent.P2P.Download;

internal class PieceBuffer(int index, FileManager fileManager) : IDisposable
{
    private readonly List<Block> _buffer = [];
    private readonly int blocksCount = fileManager.GetBlockCountByPieceIndex(index);
    public bool IsComplete => _buffer.Count == blocksCount;

    public void AddBlock(Block block)
    {
        _buffer.Add(block);
    }

    public async ValueTask<bool> WritePieceAsync(CancellationToken cancellationToken)
    {
        if (_buffer.Count != blocksCount)
        {
            return false;
        }

        using var data = _buffer
            .AsValueEnumerable()
            .OrderBy(i => i.Index)
            .Select(i => i.Payload.Memory)
            .ToArray()
            .Combine();

        var isOK = await fileManager.VerifyPieceAsync(index, data.Memory, cancellationToken);

        if (isOK)
        {
            await fileManager.WritePieceAsync(index, 0, data.Memory, cancellationToken);
        }

        return isOK;
    }

    //TODO rented array that is created based on the piece index and where blocks will be written to
    public void Dispose()
    {
        foreach (var item in _buffer)
        {
            item.Dispose();
        }
        _buffer.Clear();
    }
}
