using System.Buffers;
using Lazy;

namespace Netorrent.P2P.Structs;

internal struct MemoryRented<T>(IMemoryOwner<T> owner, int length) : IDisposable
{
    [Lazy]
    public readonly Memory<T> Memory => owner.Memory[..length];

    public readonly void Dispose() => owner.Dispose();
}
