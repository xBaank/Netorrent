using System.Buffers;

namespace Netorrent.Other;

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

    public static MemoryRented<T> From(MemoryRented<T> rented)
    {
        var owner = MemoryPool<T>.Shared.Rent(rented.Memory.Length);
        rented.Memory.CopyTo(owner.Memory);
        return new MemoryRented<T>(owner, rented.Memory.Length);
    }
}
