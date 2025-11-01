using System.Threading.Channels;

namespace Netorrent.P2P;

internal struct Request(int index, int begin, int length, CancellationToken cancellationToken)
{
    public int Index = index;
    public int Begin = begin;
    public int Length = length;
    public CancellationToken CancellationToken = cancellationToken;

    public static bool operator ==(Request left, Request right) => left.Equals(right);

    public static bool operator !=(Request left, Request right) => !(left == right);

    public override readonly bool Equals(object? obj)
    {
        return obj is Request request
            && Index == request.Index
            && Begin == request.Begin
            && Length == request.Length;
    }

    public override readonly int GetHashCode()
    {
        return HashCode.Combine(Index, Begin, Length);
    }
}

internal class RequestManager : IDisposable
{
    private readonly Channel<Request> _pendingRequests = Channel.CreateBounded<Request>(
        new BoundedChannelOptions(50) { SingleWriter = false, SingleReader = true }
    );

    public async ValueTask AddRequestAsync(Request request, CancellationToken cancellationToken)
    {
        await _pendingRequests.Writer.WriteAsync(request, cancellationToken);
    }

    public async ValueTask<Request> GetNextRequestAsync(CancellationToken cancellationToken)
    {
        return await _pendingRequests.Reader.ReadAsync(cancellationToken);
    }

    public bool IsChoking => _pendingRequests.Reader.Count >= 50;
    public bool ShouldUnchoke => _pendingRequests.Reader.Count <= 25;

    public void Dispose()
    {
        _pendingRequests.Writer.Complete();
    }
}
