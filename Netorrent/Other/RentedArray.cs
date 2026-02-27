using System.Buffers;
#if DEBUG
using System.Diagnostics;
#endif

namespace Netorrent.Other;

internal class RentedArray<T> : IDisposable
{
    private readonly Memory<T> _memory;
    private readonly T[] _array;
    private bool _disposed;

    public Memory<T> Memory =>
        _disposed ? throw new ObjectDisposedException(nameof(RentedArray<>)) : _memory;
    public int Length => _memory.Length;

    public RentedArray(int length, int start = 0)
    {
        _array = ArrayPool<T>.Shared.Rent(length);
        _memory = _array.AsMemory(start, length);
    }

#if DEBUG
    ~RentedArray()
    {
        if (!_disposed)
        {
            Debug.Fail($"Rented array was not disposed!");
            Dispose();
        }
    }
#endif

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            ArrayPool<T>.Shared.Return(_array);
#if DEBUG
            GC.SuppressFinalize(this);
#endif
        }
    }
}
