using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.TorrentFile;
using Netorrent.TorrentFile.FileStructure;
using Netorrent.Tracker.Udp.Client;
using Netorrent.Tracker.Udp.Exceptions;
using Netorrent.Tracker.Udp.Request;
using Netorrent.Tracker.Udp.Response;

namespace Netorrent.Tracker.Udp;

internal class UdpTrackerHandler : IUdpTrackerHandler
{
    private readonly ConcurrentDictionary<int, TrackerTransaction> _packetsByTransactionId = [];
    private readonly ConcurrentDictionary<long, DateTime> _connectionCreationById = [];
    private readonly ConcurrentDictionary<Guid, long> _connectionIdByTracker = [];
    private readonly List<Task> _reconnectTasks = [];
    private readonly IUdpClient _udpClient;
    private readonly ILogger _logger;
    private readonly TimeSpan _retryDelay;
    private readonly TimeSpan _retryLoopDelay;
    private readonly TimeSpan _outdatedSpan;
    private readonly int _maxRetries;
    private CancellationTokenSource? _cancellationTokenSource;
    private bool _disposed;
    private readonly Task _runTask;

    public UdpTrackerHandler(
        IUdpClient udpClient,
        ILogger logger,
        TimeSpan retryDelay,
        TimeSpan retryLoopDelay,
        TimeSpan outdatedSpan,
        int maxRetries
    )
    {
        _udpClient = udpClient;
        _logger = logger;
        _retryDelay = retryDelay;
        _retryLoopDelay = retryLoopDelay;
        _outdatedSpan = outdatedSpan;
        _maxRetries = maxRetries;
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
            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug(ex, "Gracefully stopped transaction manager");
            }
        }
    }

    private async Task ReceiveLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var result = await _udpClient.ReceiveAsync(cancellationToken).ConfigureAwait(false);

                if (result.Buffer.Length < 4)
                {
                    continue;
                }

                var actionId = BinaryPrimitives.ReadInt32BigEndian(result.Buffer);

                IUdpTrackerReceivePacket? receivedPacket = actionId switch
                {
                    UdpTrackerConnectResponse.Action => UdpTrackerConnectResponse.From(
                        result.Buffer
                    ),
                    UdpTrackerResponse.Action => UdpTrackerResponse.From(
                        result.Buffer,
                        result.RemoteEndPoint.AddressFamily
                    ),
                    UdpTrackerScrapeResponse.Action => UdpTrackerScrapeResponse.From(result.Buffer),
                    UdpTrackerErrorResponse.Action => UdpTrackerErrorResponse.From(result.Buffer),
                    _ => null,
                };

                if (receivedPacket is null)
                {
                    continue;
                }

                if (!_packetsByTransactionId.ContainsKey(receivedPacket.TransactionId))
                {
                    continue;
                }

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
                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.LogInformation(ex, "Error receiving data");
                }
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
                    if (transaction.RetryCount >= _maxRetries)
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
                            if (_reconnectTasks.Count >= 100)
                            {
                                _reconnectTasks.RemoveAll(i => i.IsCompleted);
                            }
                            _reconnectTasks.Add(
                                ReconnectAsync(transaction, udpTrackerRequest, cancellationToken)
                            );
                            continue;
                        }
                    }

                    using var payload = transaction.Packet.ToMemoryRented();
                    await _udpClient
                        .SendAsync(payload.Memory, transaction.Packet.IPEndPoint, cancellationToken)
                        .ConfigureAwait(false);
                    transaction.RetryCount++;
                    var seconds = _retryDelay * (transaction.RetryCount + 1);
                    if (seconds > 60.Seconds)
                    {
                        seconds = 60.Seconds;
                    }
                    transaction.NextRetryTime = DateTime.UtcNow + seconds;
                }
            }

            await Task.Delay(_retryLoopDelay, cancellationToken).ConfigureAwait(false);
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
            return diff > _outdatedSpan;
        }
        return true;
    }

    public async Task SendAsync(
        IUdpTrackerSendPacket packet,
        Guid trackerId,
        CancellationToken cancellationToken
    )
    {
        using var payload = packet.ToMemoryRented();

        await _udpClient
            .SendAsync(payload.Memory, packet.IPEndPoint, cancellationToken)
            .ConfigureAwait(false);
    }

    public async Task<T> SendAndReceiveAsync<T>(
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
        var seconds = _retryDelay * (transaction.RetryCount + 1);
        transaction.NextRetryTime = DateTime.UtcNow + seconds;

        await _udpClient
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
        {
            return id;
        }

        return null;
    }

    public async Task<UdpTrackerConnectResponse> ConnectAsync(
        IPEndPoint endPoint,
        Guid trackerId,
        CancellationToken cancellationToken
    )
    {
        var connectResponse = await SendAndReceiveAsync<UdpTrackerConnectResponse>(
                new UdpTrackerConnectRequest(endPoint, MakeTransactionId()),
                trackerId,
                cancellationToken
            )
            .ConfigureAwait(false);
        _connectionIdByTracker[trackerId] = connectResponse.ConnectionId;
        return connectResponse;
    }

    public async Task<ScrapeInfo?> ScrapeAsync(
        IPEndPoint endPoint,
        InfoHash infoHash,
        CancellationToken cancellationToken
    )
    {
        var trackerId = Guid.NewGuid();
        var connectionId = GetConnectionIdOrNull(trackerId);
        if (connectionId is null || IsOutdated(connectionId.Value))
        {
            var connectResponse = await ConnectAsync(endPoint, trackerId, cancellationToken)
                .ConfigureAwait(false);
            connectionId = connectResponse.ConnectionId;
        }

        var request = new UdpTrackerScrapeRequest(
            endPoint,
            connectionId.Value,
            MakeTransactionId(),
            infoHash
        );
        var response = await SendAndReceiveAsync<UdpTrackerScrapeResponse>(
                request,
                trackerId,
                cancellationToken
            )
            .ConfigureAwait(false);

        return new ScrapeInfo(response.Seeders, response.Leechers, response.Downloaded);
    }

    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _cancellationTokenSource?.Cancel();
            try
            {
                await Task.WhenAll([.. _reconnectTasks, _runTask]).ConfigureAwait(false);
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
