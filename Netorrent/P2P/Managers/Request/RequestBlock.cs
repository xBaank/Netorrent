namespace Netorrent.P2P.Managers.Request;

internal struct RequestBlock(int index, int begin, int length)
{
    public readonly int Index = index;
    public readonly int Begin = begin;
    public readonly int Length = length;
    public bool IsCancelled = false;

    public static bool operator ==(RequestBlock left, RequestBlock right) => left.Equals(right);

    public static bool operator !=(RequestBlock left, RequestBlock right) => !(left == right);

    public override readonly bool Equals(object? obj)
    {
        return obj is RequestBlock request
            && Index == request.Index
            && Begin == request.Begin
            && Length == request.Length;
    }

    public override readonly int GetHashCode()
    {
        return HashCode.Combine(Index, Begin, Length);
    }
}
