using System.Buffers;
using System.Diagnostics;

namespace Netorrent.Other;

internal class RentedArray<T>(T[] array, int length, int start = 0) : IDisposable
{
    private readonly Memory<T> memory = array.AsMemory(start, length);
    public Memory<T> Memory =>
        _disposed ? throw new ObjectDisposedException(nameof(RentedArray<>)) : memory;
    private bool _disposed;

#if DEBUG
    //  private readonly string _stacktrace = Environment.StackTrace;
#endif

    public int Length { get; } = length;

    ~RentedArray()
    {
        if (!_disposed)
        {
#if DEBUG
            // Debug.Fail($"RentedArray was not disposed! at {_stacktrace}");
            Debug.Fail($"RentedArray was not disposed!");
#endif
            Debug.WriteLine($"RentedArray was not disposed!");
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            ArrayPool<T>.Shared.Return(array);
            _disposed = true;
            GC.SuppressFinalize(this);
        }
    }
}
