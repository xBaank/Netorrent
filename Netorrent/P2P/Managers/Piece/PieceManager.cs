using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Netorrent.P2P.Managers.Request;

namespace Netorrent.P2P.Managers.Piece;

internal class PieceManager(int maxBlocks)
{
    private readonly Channel<Block> _blocksToWrite = Channel.CreateBounded<Block>(
        new BoundedChannelOptions(50) { SingleWriter = true, SingleReader = true }
    );
    private readonly Channel<RequestBlock> _requestsToSend = Channel.CreateBounded<RequestBlock>(
        new BoundedChannelOptions(maxBlocks) { SingleWriter = false, SingleReader = true }
    );
    private readonly List<RequestBlock> _sentRequests = [];

    private List<RequestBlock> _currentPieceRequests = [];

    public int? CurrentDownloadingPieceIndex { get; set; } = null;
    public bool HasFinishedCurrentPiece =>
        CurrentDownloadingPieceIndex is not null && _currentPieceRequests.Count == 0;
    public IAsyncEnumerable<Block> BlocksToWrite => _blocksToWrite.Reader.ReadAllAsync();
    public IAsyncEnumerable<RequestBlock> BlocksToRequest => GetBlocksToRequest();

    public async ValueTask SetCurrentPieceAsync(int? index, List<RequestBlock> requests)
    {
        if (index is null)
            return;

        ArgumentOutOfRangeException.ThrowIfLessThan(maxBlocks, requests.Count);

        CurrentDownloadingPieceIndex = index;
        _currentPieceRequests = requests;
        foreach (var request in _currentPieceRequests)
        {
            await _requestsToSend.Writer.WriteAsync(request);
        }
    }

    public async ValueTask AddBlockAsync(Block block, CancellationToken cancellationToken)
    {
        var toRemove = _currentPieceRequests.FirstOrDefault(r =>
            r.Index == block.Index && r.Begin == block.Begin
        );
        if (toRemove.Equals(default(RequestBlock)))
            throw new InvalidOperationException("Received unexpected block.");

        _currentPieceRequests.Remove(toRemove);
        _sentRequests.Remove(toRemove);
        await _blocksToWrite.Writer.WriteAsync(block, cancellationToken);
    }

    public async ValueTask DiscardSentRequests(CancellationToken cancellationToken)
    {
        if (_requestsToSend is null)
            return;

        foreach (var item in _sentRequests)
        {
            await _requestsToSend.Writer.WriteAsync(item, cancellationToken);
        }
    }

    private async IAsyncEnumerable<RequestBlock> GetBlocksToRequest(
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        await foreach (var request in _requestsToSend.Reader.ReadAllAsync(cancellationToken))
        {
            if (_sentRequests.Count > 8)
            {
                await Task.Delay(100, cancellationToken);
                continue;
            }
            _sentRequests.Add(request);
            yield return request;
        }
    }
}
