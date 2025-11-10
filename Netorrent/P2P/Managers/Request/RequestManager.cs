using System.Threading.Channels;

namespace Netorrent.P2P.Managers.Request;

internal class RequestManager : IAsyncDisposable
{
    private const int PEER_REQUEST_LIMIT = 8;
    private const int MAX_IGNORED_REQUESTS = 16;
    private const int MAX_VIOLATION_COUNT = 16;
    private const int MAX_BLOCK_LENGTH = 16 * 1024;

    private readonly Channel<RequestBlock> _pendingRequests = Channel.CreateBounded<RequestBlock>(
        new BoundedChannelOptions(PEER_REQUEST_LIMIT) { SingleWriter = true, SingleReader = true }
    );
    private readonly IList<RequestBlock> _requestList = [];
    private int _violationCount = 0;
    private int _ignoredRequests = 0;

    public bool IsIgnoring => _pendingRequests.Reader.Count >= PEER_REQUEST_LIMIT;
    public IAsyncEnumerable<RequestBlock> Requests =>
        _pendingRequests.Reader.ReadAllAsync().Where(i => !i.IsCancelled);

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
        _requestList.Add(request);

        return RequestResponseType.Ok;
    }

    public void CancelRequest(RequestBlock request)
    {
        var toCancel = _requestList.FirstOrDefault(i => i == request);
        if (toCancel == default)
            return;
        toCancel.IsCancelled = true;
        _requestList.Remove(request);
    }

    public ValueTask DisposeAsync()
    {
        _pendingRequests.Writer.TryComplete();
        _requestList.Clear();
        return ValueTask.CompletedTask;
    }
}
