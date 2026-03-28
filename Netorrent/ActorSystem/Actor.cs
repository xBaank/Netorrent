using System.Threading.Channels;

namespace Netorrent.ActorSystem;

internal abstract class Actor<TMessage> : IAsyncDisposable
{
    private readonly Channel<TMessage> _mailbox = Channel.CreateBounded<TMessage>(
        new BoundedChannelOptions(128) { SingleWriter = false, SingleReader = true }
    );
    private readonly List<Timer> _timers = [];
    private CancellationTokenSource? _cts;
    private Task? _runningTask;
    private bool _disposed;

    protected ChannelReader<TMessage> MailboxReader => _mailbox.Reader;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_runningTask is not null)
        {
            throw new InvalidOperationException("Actor already started.");
        }
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _runningTask = RunAsync(_cts.Token);
        return _runningTask;
    }

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        try
        {
            await OnStartedAsync(cancellationToken).ConfigureAwait(false);
            await foreach (
                var message in _mailbox.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false)
            )
            {
                await OnReceiveAsync(message, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            DisposeTimers();
            await OnStoppingAsync().ConfigureAwait(false);
        }
    }

    protected abstract ValueTask OnReceiveAsync(
        TMessage message,
        CancellationToken cancellationToken
    );

    protected virtual Task OnStartedAsync(CancellationToken cancellationToken) =>
        Task.CompletedTask;

    protected virtual ValueTask OnStoppingAsync() => ValueTask.CompletedTask;

    protected bool Tell(TMessage message) => _mailbox.Writer.TryWrite(message);

    protected ValueTask SendAsync(TMessage message, CancellationToken cancellationToken) =>
        _mailbox.Writer.WriteAsync(message, cancellationToken);

    protected void ScheduleRepeatedly(TimeSpan delay, TimeSpan interval, TMessage message)
    {
        if (delay == TimeSpan.Zero)
        {
            _mailbox.Writer.TryWrite(message);
            delay = interval;
        }
        var timer = new Timer(_ => _mailbox.Writer.TryWrite(message), null, delay, interval);
        _timers.Add(timer);
    }

    protected virtual ValueTask DrainAsync() => ValueTask.CompletedTask;

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
            await DrainAsync().ConfigureAwait(false);
            _cts?.Dispose();
        }
    }
}
