using System.Buffers;
using Lazy;

namespace Netorrent.P2P.Structs;

internal struct MemoryRented<T>(IMemoryOwner<T> owner, int length) : IDisposable
{
    public readonly Memory<T> Memory => owner.Memory[..length];

    public readonly void Dispose() => owner.Dispose();

    public static MemoryRented<T> From(T[] rented)
    {
        var owner = MemoryPool<T>.Shared.Rent(rented.Length);
        rented.AsSpan().CopyTo(owner.Memory.Span);
        return new MemoryRented<T>(owner, rented.Length);
    }
}
