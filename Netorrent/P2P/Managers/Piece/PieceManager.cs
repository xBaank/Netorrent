using System.Threading.Channels;
using Netorrent.P2P.Managers.Request;

namespace Netorrent.P2P.Managers.Piece;

internal class PieceManager()
{
    private readonly Channel<Block> _blocksToWrite = Channel.CreateBounded<Block>(
        new BoundedChannelOptions(50) { SingleWriter = true, SingleReader = true }
    );
    public int? CurrentDownloadingPieceIndex { get; set; } = null;
    private List<RequestBlock> _currentPieceRequests = [];

    public bool HasFinishedCurrentPiece =>
        CurrentDownloadingPieceIndex is not null && _currentPieceRequests.Count == 0;

    public IAsyncEnumerable<Block> BlocksToWrite => _blocksToWrite.Reader.ReadAllAsync();

    public void SetCurrentPiece(int? index, List<RequestBlock> requests)
    {
        CurrentDownloadingPieceIndex = index;
        if (index is not null)
            _currentPieceRequests = requests;
    }

    public async ValueTask AddBlockAsync(Block block)
    {
        var toRemove = _currentPieceRequests.FirstOrDefault(r =>
            r.Index == block.Index && r.Begin == block.Begin
        );
        if (toRemove.Equals(default(RequestBlock)))
            throw new InvalidOperationException("Received unexpected block.");

        _currentPieceRequests.Remove(toRemove);
        await _blocksToWrite.Writer.WriteAsync(block);
    }
}
