using System.Collections.Concurrent;
using System.Threading.Channels;
using Netorrent.P2P.Messages;

namespace Netorrent.P2P.Upload;

internal class UploadScheduler : IAsyncDisposable
{
    private const int PEER_REQUEST_LIMIT = 8;
    private const int MAX_IGNORED_REQUESTS = 16;
    private const int MAX_VIOLATION_COUNT = 16;
    private const int MAX_BLOCK_LENGTH = 16 * 1024;

    private readonly Channel<RequestBlock> _pendingRequests = Channel.CreateBounded<RequestBlock>(
        new BoundedChannelOptions(PEER_REQUEST_LIMIT) { SingleWriter = true, SingleReader = true }
    );
    private readonly ConcurrentDictionary<
        (int Index, int Begin, int Length),
        RequestBlock
    > _requestByIBL = [];
    private int _violationCount = 0;
    private int _ignoredRequests = 0;

    public bool IsIgnoring => _pendingRequests.Reader.Count >= PEER_REQUEST_LIMIT;
    public IAsyncEnumerable<RequestBlock> Requests =>
        _pendingRequests.Reader.ReadAllAsync().Where(i => i.State != RequestBlockState.Cancelled);

    //TODO calculate max based on upload speed and latency
    public async ValueTask<RequestResponseType> AddRequestAsync(
        RequestBlock request,
        CancellationToken cancellationToken
    )
    {
        if (_violationCount >= MAX_VIOLATION_COUNT)
            return RequestResponseType.Violation;

        if (IsIgnoring)
        {
            _ignoredRequests++;
            if (_ignoredRequests >= MAX_IGNORED_REQUESTS)
                return RequestResponseType.Ignored;
        }

        if (
            request.Length <= 0
            || request.Length > MAX_BLOCK_LENGTH
            || request.Index < 0
            || request.Index >= request.Length
        )
        {
            _violationCount++;
            return RequestResponseType.Ignored;
        }

        _ignoredRequests = 0;
        _violationCount = 0;

        await _pendingRequests.Writer.WriteAsync(request, cancellationToken);
        var key = (request.Index, request.Begin, request.Length);
        _requestByIBL.TryAdd(key, request);

        return RequestResponseType.Ok;
    }

    public void CancelRequest(RequestBlock request)
    {
        var key = (request.Index, request.Begin, request.Length);
        if (!_requestByIBL.TryGetValue(key, out var requestBlock))
            return;

        requestBlock.State = RequestBlockState.Cancelled;
        _requestByIBL.TryRemove(key, out _);
    }

    public ValueTask DisposeAsync()
    {
        _pendingRequests.Writer.TryComplete();
        _requestByIBL.Clear();
        return ValueTask.CompletedTask;
    }
}
