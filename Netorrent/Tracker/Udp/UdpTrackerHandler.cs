using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.Tracker.Udp.Client;
using Netorrent.Tracker.Udp.Exceptions;
using Netorrent.Tracker.Udp.Request;
using Netorrent.Tracker.Udp.Response;

namespace Netorrent.Tracker.Udp;

internal class UdpTrackerHandler(
    IUdpClient udpClient,
    ILogger logger,
    TimeSpan retryDelay,
    TimeSpan retryLoopDelay,
    int maxRetries
) : IUdpTrackerHandler
{
    private readonly ConcurrentDictionary<int, TrackerTransaction> _packetsByTransactionId = [];
    private readonly ConcurrentDictionary<long, DateTime> _connectionCreationById = [];
    private readonly ConcurrentDictionary<Guid, long> _connectionIdByTracker = [];
    private CancellationTokenSource? _cancellationTokenSource;
    private bool _disposed;
    private Task? _runTask;

    public void Start()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _runTask = StartAsync();
    }

    private async Task StartAsync()
    {
        try
        {
            _cancellationTokenSource = new();
            await Task.WhenAll(
                    ReceiveLoopAsync(_cancellationTokenSource.Token),
                    RetryLoopAsync(_cancellationTokenSource.Token)
                )
                .ConfigureAwait(false);
        }
        catch (TaskCanceledException ex)
        {
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug(ex, "Gracefully stopped transaction manager");
        }
    }

    private async Task ReceiveLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var result = await udpClient.ReceiveAsync(cancellationToken).ConfigureAwait(false);

                if (result.Buffer.Length < 4)
                    continue;

                var actionId = BinaryPrimitives.ReadInt32BigEndian(result.Buffer);

                IUdpTrackerReceivePacket? receivedPacket = actionId switch
                {
                    UdpTrackerConnectResponse.Action => UdpTrackerConnectResponse.From(
                        result.Buffer
                    ),
                    UdpTrackerResponse.Action => UdpTrackerResponse.From(
                        result.Buffer,
                        result.RemoteEndPoint.AddressFamily == AddressFamily.InterNetwork
                        || result.RemoteEndPoint.Address.IsIPv4MappedToIPv6
                            ? AddressFamily.InterNetwork
                            : AddressFamily.InterNetworkV6
                    ),
                    UdpTrackerErrorResponse.Action => UdpTrackerErrorResponse.From(result.Buffer),
                    _ => null,
                };

                if (receivedPacket is null)
                    continue;

                if (receivedPacket is UdpTrackerConnectResponse udpTrackerConnectResponse)
                {
                    _connectionCreationById[udpTrackerConnectResponse.ConnectionId] =
                        DateTime.UtcNow;
                }

                if (
                    _packetsByTransactionId.TryGetValue(
                        receivedPacket.TransactionId,
                        out var packet
                    )
                )
                {
                    if (receivedPacket is UdpTrackerErrorResponse udpTrackerErrorResponse)
                    {
                        packet.Response.TrySetException(
                            new UdpTrackerException(udpTrackerErrorResponse.Message)
                        );
                    }
                    else
                    {
                        packet.Response.TrySetResult(receivedPacket);
                    }
                    _packetsByTransactionId.TryRemove(receivedPacket.TransactionId, out _);
                }
            }
            catch (Exception ex)
            {
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogInformation(ex, "Error receiving data");
            }
        }
    }

    private async Task RetryLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            foreach (var transaction in _packetsByTransactionId.Values)
            {
                var now = DateTime.UtcNow;
                if (now > transaction.NextRetryTime)
                {
                    if (transaction.RetryCount >= maxRetries)
                    {
                        transaction.Response.TrySetException(
                            new TimeoutException("Tracker did not respond")
                        );
                        _packetsByTransactionId.TryRemove(transaction.Packet.TransactionId, out _);
                        continue;
                    }

                    if (transaction.Packet is UdpTrackerRequest udpTrackerRequest)
                    {
                        if (IsOutdated(udpTrackerRequest.ConnectionId))
                        {
                            _ = ReconnectAsync(transaction, udpTrackerRequest, cancellationToken);
                            continue;
                        }
                    }

                    using var payload = transaction.Packet.ToMemoryRented();
                    await udpClient
                        .SendAsync(payload.Memory, transaction.Packet.IPEndPoint, cancellationToken)
                        .ConfigureAwait(false);
                    transaction.RetryCount++;
                    var seconds = retryDelay * (transaction.RetryCount + 1);
                    transaction.NextRetryTime = DateTime.UtcNow + seconds;
                }
            }

            await Task.Delay(retryLoopDelay, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task ReconnectAsync(
        TrackerTransaction transaction,
        UdpTrackerRequest udpTrackerRequest,
        CancellationToken cancellationToken
    )
    {
        try
        {
            _packetsByTransactionId.TryRemove(transaction.Packet.TransactionId, out _);

            var newConnectionResponse = await ConnectAsync(
                    udpTrackerRequest.IPEndPoint,
                    transaction.TrackerId,
                    cancellationToken
                )
                .ConfigureAwait(false);
            var newPacket = udpTrackerRequest with
            {
                ConnectionId = newConnectionResponse.ConnectionId,
            };
            var newTransaction = transaction with { Packet = newPacket };

            _packetsByTransactionId.TryAdd(newTransaction.Packet.TransactionId, newTransaction);
        }
        catch (Exception ex)
        {
            transaction.Response.SetException(ex);
        }
    }

    private TrackerTransaction RegisterOrGetTransaction(
        IUdpTrackerSendPacket packet,
        Guid trackerId,
        CancellationToken cancellationToken,
        out bool isNew
    )
    {
        var task = new TaskCompletionSource<IUdpTrackerReceivePacket>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        cancellationToken.Register(() => task.TrySetCanceled());
        var transaction = new TrackerTransaction(packet, task, trackerId);

        if (_packetsByTransactionId.TryAdd(packet.TransactionId, transaction))
        {
            isNew = true;
            return transaction;
        }

        _packetsByTransactionId.TryGetValue(packet.TransactionId, out var existing);
        isNew = false;
        return existing!;
    }

    public bool IsOutdated(long connectionId)
    {
        if (_connectionCreationById.TryGetValue(connectionId, out var creationTime))
        {
            var diff = DateTime.UtcNow - creationTime;
            return diff > 1.Minutes;
        }
        return true;
    }

    public async Task<T> SendAsync<T>(
        IUdpTrackerSendPacket packet,
        Guid trackerId,
        CancellationToken cancellationToken
    )
        where T : IUdpTrackerReceivePacket
    {
        var transaction = RegisterOrGetTransaction(
            packet,
            trackerId,
            cancellationToken,
            out var isNew
        );

        if (!isNew)
        {
            return (T)await transaction.Response.Task.ConfigureAwait(false);
        }

        using var payload = packet.ToMemoryRented();
        var seconds = retryLoopDelay * (transaction.RetryCount + 1);
        transaction.NextRetryTime = DateTime.UtcNow + seconds;

        await udpClient
            .SendAsync(payload.Memory, packet.IPEndPoint, cancellationToken)
            .ConfigureAwait(false);

        return (T)await transaction.Response.Task.ConfigureAwait(false);
    }

    public int MakeTransactionId()
    {
        int id;
        do
        {
            id = Random.Shared.Next();
        } while (_packetsByTransactionId.ContainsKey(id));
        return id;
    }

    public long? GetConnectionIdOrNull(Guid trackerId)
    {
        if (_connectionIdByTracker.TryGetValue(trackerId, out var id))
            return id;

        return null;
    }

    public async Task<UdpTrackerConnectResponse> ConnectAsync(
        IPEndPoint endPoint,
        Guid trackerId,
        CancellationToken cancellationToken
    )
    {
        var connectResponse = await SendAsync<UdpTrackerConnectResponse>(
                new UdpTrackerConnectRequest(endPoint, MakeTransactionId()),
                trackerId,
                cancellationToken
            )
            .ConfigureAwait(false);
        _connectionIdByTracker[trackerId] = connectResponse.ConnectionId;
        return connectResponse;
    }

    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _cancellationTokenSource?.Cancel();
            try
            {
                if (_runTask is not null)
                {
                    await _runTask.ConfigureAwait(false);
                }
            }
            catch { }
            _packetsByTransactionId.Clear();
            _connectionCreationById.Clear();
            _connectionIdByTracker.Clear();
            _cancellationTokenSource?.Dispose();
            _disposed = true;
        }
    }
}
