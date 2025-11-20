using System.Buffers;
using System.Runtime.CompilerServices;

namespace Netorrent.Other;

internal class RentedArray<T>(T[] array, int length, int start = 0) : IDisposable
{
    public readonly Memory<T> Memory = array.AsMemory(start, length);
    private readonly T[] _array = array;
    private bool _disposed;
    public int Length { get; } = length;
    public int Start { get; } = start;

    public void Dispose()
    {
        if (!_disposed)
        {
            ArrayPool<T>.Shared.Return(
                _array,
                clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<T>()
            );
            _disposed = true;
        }
    }
}
