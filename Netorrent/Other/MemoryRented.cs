using System.Buffers;

namespace Netorrent.Other;

internal struct MemoryRented<T>(IMemoryOwner<T> owner, int length, int start = 0) : IDisposable
{
    public readonly Memory<T> Memory => owner.Memory.Slice(start, length);

    public readonly void Dispose() => owner.Dispose();
}
