using System.Buffers;
using System.Diagnostics;

namespace Netorrent.Other;

internal class RentedArray<T>(T[] array, int length, int start = 0) : IDisposable
{
    public readonly Memory<T> Memory = array.AsMemory(start, length);
    private readonly T[] _array = array;
    private bool _disposed;

    public int Length { get; } = length;
    public int Start { get; } = start;

    //TODO fix this cases where rented array is not disposed
    ~RentedArray()
    {
        if (!_disposed)
        {
            ArrayPool<T>.Shared.Return(_array);
#if DEBUG
            Debug.Fail("RentedArray was not disposed!");
#endif
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            ArrayPool<T>.Shared.Return(_array);
            _disposed = true;
            GC.SuppressFinalize(this); // prevent finalizer from running
        }
    }
}
