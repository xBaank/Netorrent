using System.Collections.Concurrent;
using System.Net;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Dht.Krpc;
using Netorrent.Extensions;
using Netorrent.Tracker.Udp.Client;

namespace Netorrent.Dht;

internal sealed class DhtHandler : IDhtHandler
{
    private readonly ConcurrentDictionary<ushort, DhtTransaction> _pending = new();
    private readonly IUdpClient _udpClient;
    private readonly ILogger _logger;
    private readonly TimeSpan _retryDelay;
    private readonly TimeSpan _retryLoopDelay;
    private readonly int _maxRetries;
    private CancellationTokenSource? _cancellationTokenSource;
    private readonly Task _runTask;
    private bool _disposed;

    // Unsolicited inbound messages (incoming queries + unmatched responses). Best-effort:
    // the receive loop must never block, so a full buffer drops the oldest entries rather
    // than stalling UDP intake.
    private readonly Channel<(KrpcMessage Message, IPEndPoint Remote)> _incoming =
        Channel.CreateBounded<(KrpcMessage, IPEndPoint)>(
            new BoundedChannelOptions(1024)
            {
                SingleReader = true,
                SingleWriter = true,
                FullMode = BoundedChannelFullMode.DropOldest,
            }
        );

    public ChannelReader<(KrpcMessage Message, IPEndPoint Remote)> IncomingMessages =>
        _incoming.Reader;

    public DhtHandler(
        IUdpClient udpClient,
        ILogger logger,
        TimeSpan retryDelay,
        TimeSpan retryLoopDelay,
        int maxRetries
    )
    {
        _udpClient = udpClient;
        _logger = logger;
        _retryDelay = retryDelay;
        _retryLoopDelay = retryLoopDelay;
        _maxRetries = maxRetries;
        _runTask = StartInternalAsync();
    }

    private async Task StartInternalAsync()
    {
        try
        {
            _cancellationTokenSource = new CancellationTokenSource();
            await Task.WhenAll(
                    ReceiveLoopAsync(_cancellationTokenSource.Token),
                    RetryLoopAsync(_cancellationTokenSource.Token)
                )
                .ConfigureAwait(false);
        }
        catch (TaskCanceledException ex)
        {
            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug(ex, "DHT handler stopped");
        }
    }

    private async Task ReceiveLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var result = await _udpClient.ReceiveAsync(ct).ConfigureAwait(false);

                var msg = await KrpcSerializer
                    .TryDeserializeAsync(result.Buffer, ct)
                    .ConfigureAwait(false);
                if (msg is null)
                    continue;

                // A response to one of our own queries resolves its awaiter and stops here.
                // Anything else (incoming queries, or stray/late responses) is published as
                // unsolicited inbound traffic for the DHT client to handle.
                if (_pending.TryRemove(msg.TransactionId.Value, out var tx))
                    tx.Response.TrySetResult(msg);
                else
                    _incoming.Writer.TryWrite((msg, result.RemoteEndPoint));
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug(ex, "Error in DHT receive loop");
            }
        }
    }

    private async Task RetryLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            foreach (var tx in _pending.Values)
            {
                if (DateTime.UtcNow <= tx.NextRetryTime)
                    continue;

                if (tx.RetryCount >= _maxRetries)
                {
                    tx.Response.TrySetException(new TimeoutException("DHT node did not respond"));
                    _pending.TryRemove(tx.Message.TransactionId.Value, out _);
                    continue;
                }

                try
                {
                    using var payload = await KrpcSerializer
                        .SerializeAsync(tx.Message, ct)
                        .ConfigureAwait(false);
                    await _udpClient.SendAsync(payload.Memory, tx.Remote, ct).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    if (_logger.IsEnabled(LogLevel.Debug))
                        _logger.LogDebug(ex, "Error retrying DHT packet");
                }

                tx.RetryCount++;
                var delay = _retryDelay * (tx.RetryCount + 1);
                if (delay > 60.Seconds)
                    delay = 60.Seconds;
                tx.NextRetryTime = DateTime.UtcNow + delay;
            }

            await Task.Delay(_retryLoopDelay, ct).ConfigureAwait(false);
        }
    }

    public async ValueTask SendAsync(
        KrpcMessage message,
        IPEndPoint remote,
        CancellationToken cancellationToken
    )
    {
        using var payload = await KrpcSerializer
            .SerializeAsync(message, cancellationToken)
            .ConfigureAwait(false);
        await _udpClient.SendAsync(payload.Memory, remote, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<KrpcMessage> SendAndReceiveAsync(
        KrpcMessage query,
        IPEndPoint remote,
        CancellationToken cancellationToken
    )
    {
        var tcs = new TaskCompletionSource<KrpcMessage>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        await using var reg = cancellationToken.Register(() =>
            tcs.TrySetCanceled(cancellationToken)
        );

        var tx = new DhtTransaction(query, remote, tcs);
        tx.NextRetryTime = DateTime.UtcNow + _retryDelay;

        if (!_pending.TryAdd(query.TransactionId.Value, tx))
        {
            // Collision: regenerate transaction ID and retry once
            var newTxId = MakeTransactionId();
            var newQuery = query with { TransactionId = newTxId };
            tx = new DhtTransaction(newQuery, remote, tcs);
            tx.NextRetryTime = DateTime.UtcNow + _retryDelay;
            _pending[newTxId.Value] = tx;
            query = newQuery;
        }

        try
        {
            using var payload = await KrpcSerializer
                .SerializeAsync(query, cancellationToken)
                .ConfigureAwait(false);
            await _udpClient
                .SendAsync(payload.Memory, remote, cancellationToken)
                .ConfigureAwait(false);

            return await tcs.Task.ConfigureAwait(false);
        }
        finally
        {
            _pending.TryRemove(query.TransactionId.Value, out _);
        }
    }

    private TransactionId MakeTransactionId()
    {
        ushort id;
        do
        {
            id = (ushort)Random.Shared.Next(0, 65536);
        } while (_pending.ContainsKey(id));
        return new TransactionId(id);
    }

    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            _cancellationTokenSource?.Cancel();
            _incoming.Writer.TryComplete();
            try
            {
                await _runTask.ConfigureAwait(false);
            }
            catch { }
            foreach (var tx in _pending.Values)
                tx.Response.TrySetCanceled();
            _pending.Clear();
            _cancellationTokenSource?.Dispose();
        }
    }
}
