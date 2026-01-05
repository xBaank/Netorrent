using System.Buffers;
using System.Diagnostics;
using Netorrent.Extensions;

namespace Netorrent.Other;

internal class RentedArray<T>(T[] array, int length, int start = 0) : IDisposable
{
    private readonly Memory<T> memory = array.AsMemory(start, length);
    public Memory<T> Memory =>
        _disposed ? throw new ObjectDisposedException(nameof(RentedArray<>)) : memory;
    private bool _disposed;

    public int Length => memory.Length;

    ~RentedArray()
    {
        if (!_disposed)
        {
#if DEBUG
            Debug.Fail("Rented array was not diposed!");
#else
            Debug.WriteLine("Rented array was not diposed!");
#endif
            Dispose();
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            ArrayPool<T>.Shared.Return(array);
            GC.SuppressFinalize(this);
        }
    }
}
