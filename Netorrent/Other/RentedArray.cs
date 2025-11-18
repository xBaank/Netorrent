using System.Buffers;

namespace Netorrent.Other;

internal struct RentedArray<T>(T[] array, int length, int start = 0) : IDisposable
{
    public readonly Memory<T> Memory => array.AsMemory().Slice(start, length);

    public readonly void Dispose()
    {
        ArrayPool<T>.Shared.Return(array);
    }
}
