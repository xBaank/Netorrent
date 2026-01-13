using System.Buffers;
using System.Diagnostics;
using Netorrent.Extensions;

namespace Netorrent.Other;

internal class RentedArray<T>(
    T[] array,
    int length,
    int start = 0
#if DEBUG
    ,
    [System.Runtime.CompilerServices.CallerFilePath] string sourceFilePath = "",
    [System.Runtime.CompilerServices.CallerLineNumber] int sourceLineNumber = 0
#endif
) : IDisposable
{
    private readonly Memory<T> _memory = array.AsMemory(start, length);
    private bool _disposed;
    public Memory<T> Memory =>
        _disposed ? throw new ObjectDisposedException(nameof(RentedArray<>)) : _memory;
    public int Length => _memory.Length;

    ~RentedArray()
    {
        if (!_disposed)
        {
#if DEBUG
            Debug.Fail($"Rented array was not diposed! at {sourceFilePath} {sourceLineNumber}");
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
