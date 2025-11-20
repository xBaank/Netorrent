using Netorrent.P2P.Download;

namespace Netorrent.P2P.Messages;

enum RequestBlockState
{
    Pending,
    Requested,
    Cancelled,
    Received,
}

internal class RequestBlock(int index, int begin, int length)
{
    public readonly int Index = index;
    public readonly int Begin = begin;
    public readonly int Length = length;
    public RequestBlockState State { get; set; } = RequestBlockState.Pending;
    public PeerConnection? RequestedFrom { get; set; } ;
    public DateTimeOffset? RequestedAt { get; set; }

    public static bool operator ==(RequestBlock left, RequestBlock right) => left.Equals(right);

    public static bool operator !=(RequestBlock left, RequestBlock right) => !(left == right);

    public override bool Equals(object? obj)
    {
        return obj is RequestBlock request
            && Index == request.Index
            && Begin == request.Begin
            && Length == request.Length;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Index, Begin, Length);
    }
}
