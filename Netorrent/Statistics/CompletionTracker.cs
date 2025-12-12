using System.Runtime.CompilerServices;
using Netorrent.P2P.Messages;

namespace Netorrent.Statistics;

public class CompletionTracker
{
    private readonly Bitfield _bitfield;
    private readonly IDisposable _stateDisposable;
    private TaskCompletionSource _downloadTaskCompletitionSource = new();

    internal CompletionTracker(Bitfield bitfield)
    {
        _bitfield = bitfield;
        _stateDisposable = _bitfield.StateChanged.Subscribe(CheckDownload);
    }

    internal void Reset()
    {
        _downloadTaskCompletitionSource.TrySetCanceled();
        _downloadTaskCompletitionSource = new();
        if (_bitfield.IsComplete)
            _downloadTaskCompletitionSource.TrySetResult();
    }

    internal void TrySetException(Exception exception)
    {
        _downloadTaskCompletitionSource.TrySetException(exception);
    }

    internal void TrySetCanceled()
    {
        _downloadTaskCompletitionSource.TrySetCanceled();
    }

    private void CheckDownload(int pieceIndex)
    {
        if (_bitfield.IsComplete)
        {
            _downloadTaskCompletitionSource.TrySetResult();
        }
    }

    internal void Dispose()
    {
        _stateDisposable.Dispose();
    }

    public TaskAwaiter GetAwaiter() => _downloadTaskCompletitionSource.Task.GetAwaiter();

    public Task AsTask() => _downloadTaskCompletitionSource.Task;
}
