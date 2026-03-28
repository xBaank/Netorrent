using System.Threading.Channels;

namespace Netorrent.ActorSystem;

internal sealed class Actor<TMessage> : IAsyncDisposable
{
    private readonly Channel<TMessage> _mailbox = Channel.CreateBounded<TMessage>(
        new BoundedChannelOptions(128) { SingleWriter = false, SingleReader = true }
    );
    private readonly List<Timer> _timers = [];
    private CancellationTokenSource? _cts;
    private Task? _runningTask;
    private bool _disposed;

    public ChannelReader<TMessage> MailboxReader => _mailbox.Reader;

    public Task StartAsync(
        Func<TMessage, CancellationToken, ValueTask> onReceive,
        CancellationToken cancellationToken
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_runningTask is { IsCompleted: false })
        {
            throw new InvalidOperationException("Actor already started.");
        }
        _cts?.Dispose();
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _runningTask = RunAsync(onReceive, _cts.Token);
        return _runningTask;
    }

    private async Task RunAsync(
        Func<TMessage, CancellationToken, ValueTask> onReceive,
        CancellationToken cancellationToken
    )
    {
        try
        {
            await foreach (
                var message in _mailbox.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false)
            )
            {
                await onReceive(message, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            DisposeTimers();
        }
    }

    public bool Tell(TMessage message) => _mailbox.Writer.TryWrite(message);

    public ValueTask SendAsync(TMessage message, CancellationToken cancellationToken) =>
        _mailbox.Writer.WriteAsync(message, cancellationToken);

    public void ScheduleRepeatedly(TimeSpan delay, TimeSpan interval, TMessage message)
    {
        if (delay == TimeSpan.Zero)
        {
            _mailbox.Writer.TryWrite(message);
            delay = interval;
        }
        var timer = new Timer(_ => _mailbox.Writer.TryWrite(message), null, delay, interval);
        _timers.Add(timer);
    }

    private void DisposeTimers()
    {
        foreach (var timer in _timers)
        {
            timer.Dispose();
        }
        _timers.Clear();
    }

    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            _cts?.Cancel();
            _mailbox.Writer.TryComplete();

            try
            {
                if (_runningTask is not null)
                {
                    await _runningTask.ConfigureAwait(false);
                }
            }
            catch { }

            DisposeTimers();
            _cts?.Dispose();
        }
    }
}
