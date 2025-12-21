using Netorrent.Extensions;
using ZLinq;

namespace Netorrent.Other;

internal class ReadOnlyMemoryEqualityComparer<T> : IEqualityComparer<ReadOnlyMemory<T>>
{
    public static ReadOnlyMemoryEqualityComparer<T> Instance { get; } = new();

    public bool Equals(ReadOnlyMemory<T> x, ReadOnlyMemory<T> y)
    {
        return x.Span.SequenceEqual(y.Span);
    }

    public int GetHashCode(ReadOnlyMemory<T> obj)
    {
        var hash = new HashCode();
        foreach (var b in obj.Span)
        {
            hash.Add(b);
        }
        return hash.ToHashCode();
    }
}
