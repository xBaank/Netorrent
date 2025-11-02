using System.Threading.Channels;

namespace Netorrent.P2P.Managers.Request;

internal class RequestManager : IDisposable
{
    private readonly Channel<Request> _pendingRequests = Channel.CreateBounded<Request>(
        new BoundedChannelOptions(50) { SingleWriter = false, SingleReader = true }
    );

    private readonly IList<Request> _requestList = [];

    private const int PEER_REQUEST_LIMIT = 8;
    private const int MAX_IGNORED_REQUESTS = 16;
    private const int MAX_VIOLATION_COUNT = 16;
    private const int MAX_BLOCK_LENGTH = 16 * 1024;

    private int _violationCount = 0;
    private int _ignoredRequests = 0;

    public async ValueTask<RequestResponseType> AddRequestAsync(
        Request request,
        CancellationToken cancellationToken
    )
    {
        if (_violationCount >= MAX_VIOLATION_COUNT)
            return RequestResponseType.Violation;

        if (IsChoking)
        {
            _violationCount++;
            return RequestResponseType.Choked;
        }

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

    public void CancelRequest(Request request)
    {
        var toCancel = _requestList.FirstOrDefault(i => i == request);
        if (toCancel == default)
            return;
        toCancel.IsCancelled = true;
        _requestList.Remove(request);
    }

    public bool IsChoking =>
        _pendingRequests.Reader.Count >= MAX_IGNORED_REQUESTS + PEER_REQUEST_LIMIT;
    public bool IsIgnoring => _pendingRequests.Reader.Count >= PEER_REQUEST_LIMIT;
    public bool ShouldUnchoke => _pendingRequests.Reader.Count <= 4;
    public IAsyncEnumerable<Request> Requests =>
        _pendingRequests.Reader.ReadAllAsync().Where(i => !i.IsCancelled);

    public void Dispose()
    {
        _pendingRequests.Writer.Complete();
    }
}
