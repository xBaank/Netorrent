using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Netorrent.P2P.Managers.Request;

namespace Netorrent.P2P.Managers.Piece;

internal class PieceManager(int maxBlocks) : IAsyncDisposable
{
    private readonly Channel<Block> _blocksToWrite = Channel.CreateBounded<Block>(
        new BoundedChannelOptions(50) { SingleWriter = true, SingleReader = true }
    );
    private readonly Channel<RequestBlock> _requestsToSend = Channel.CreateBounded<RequestBlock>(
        new BoundedChannelOptions(maxBlocks) { SingleWriter = false, SingleReader = true }
    );
    private readonly ConcurrentDictionary<
        (int Index, int Begin, int Length),
        RequestBlock
    > _sentRequestsByIBL = [];

    private ConcurrentDictionary<
        (int Index, int Begin, int Length),
        RequestBlock
    > _currentPieceRequestsByIBL = [];

    public int? CurrentDownloadingPieceIndex { get; set; } = null;
    public bool HasFinishedCurrentPiece =>
        CurrentDownloadingPieceIndex is not null && _currentPieceRequestsByIBL.IsEmpty;
    public IAsyncEnumerable<Block> BlocksToWrite => _blocksToWrite.Reader.ReadAllAsync();
    public IAsyncEnumerable<RequestBlock> BlocksToRequest => GetBlocksToRequest();

    public async ValueTask SetCurrentPieceAsync(
        int index,
        List<RequestBlock> requests,
        CancellationToken cancellationToken
    )
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxBlocks, requests.Count);

        CurrentDownloadingPieceIndex = index;
        foreach (var request in requests)
        {
            _currentPieceRequestsByIBL.TryAdd(
                (request.Index, request.Begin, request.Length),
                request
            );
            await _requestsToSend.Writer.WriteAsync(request, cancellationToken);
        }
    }

    public async ValueTask AddBlockAsync(Block block, CancellationToken cancellationToken)
    {
        var key = (block.Index, block.Begin, block.Payload.Memory.Length);

        if (!_currentPieceRequestsByIBL.ContainsKey(key))
            return;

        _sentRequestsByIBL.TryRemove(key, out _);
        await _blocksToWrite.Writer.WriteAsync(block, cancellationToken);
    }

    public void SetBlockWritten(Block block)
    {
        var key = (block.Index, block.Begin, block.Payload.Memory.Length);

        if (!_currentPieceRequestsByIBL.ContainsKey(key))
            return;

        _currentPieceRequestsByIBL.TryRemove(key, out _);
    }

    public async ValueTask DiscardSentRequests(CancellationToken cancellationToken)
    {
        if (_requestsToSend is null)
            return;

        foreach (var item in _sentRequestsByIBL)
        {
            await _requestsToSend.Writer.WriteAsync(item.Value, cancellationToken);
        }
    }

    private async IAsyncEnumerable<RequestBlock> GetBlocksToRequest(
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        await foreach (var request in _requestsToSend.Reader.ReadAllAsync(cancellationToken))
        {
            while (!cancellationToken.IsCancellationRequested && _sentRequestsByIBL.Count > 8)
            {
                await Task.Delay(50, cancellationToken);
            }
            var key = (request.Index, request.Begin, request.Length);
            _sentRequestsByIBL.TryAdd(key, request);
            yield return request;
        }
    }

    public async ValueTask DisposeAsync()
    {
        _blocksToWrite.Writer.TryComplete();
        _requestsToSend.Writer.TryComplete();
        await foreach (var item in _blocksToWrite.Reader.ReadAllAsync())
        {
            item.Dispose();
        }
        await foreach (var item in _requestsToSend.Reader.ReadAllAsync()) { }
        _sentRequestsByIBL.Clear();
        _currentPieceRequestsByIBL.Clear();
    }
}
