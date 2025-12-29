using System.Buffers;
using Netorrent.Extensions;

namespace Netorrent.Other;

internal struct RentedArray<T>(T[] array, int length, int start = 0) : IDisposable
{
    private readonly Memory<T> memory = array.AsMemory(start, length);
    public readonly Memory<T> Memory =>
        _disposed ? throw new ObjectDisposedException(nameof(RentedArray<>)) : memory;
    private bool _disposed;

    public readonly int Length => memory.Length;

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            ArrayPool<T>.Shared.Return(array);
        }
    }
}
