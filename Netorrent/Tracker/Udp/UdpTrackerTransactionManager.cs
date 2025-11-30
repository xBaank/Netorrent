using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.Tracker.Udp.Request;
using Netorrent.Tracker.Udp.Response;

namespace Netorrent.Tracker.Udp;

internal class UdpTrackerTransactionManager(UdpClient udpClient, ILogger logger) : IDisposable
{
    private const int MAX_RETRIES = 8;
    private readonly ConcurrentDictionary<int, TrackerTransaction> _packetsByTransactionId = [];
    private readonly ConcurrentDictionary<long, DateTime> _connectionCreationById = [];
    private readonly ConcurrentDictionary<Guid, long> _connectionIdByTracker = [];

    public Task? TrackerManagerTask { get; private set; }

    public void Start(CancellationToken cancellationToken)
    {
        TrackerManagerTask = Task.WhenAll(
            ReceiveLoopAsync(cancellationToken),
            RetryLoopAsync(cancellationToken)
        );
    }

    private async Task ReceiveLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var result = await udpClient.ReceiveAsync(cancellationToken);

                if (result.Buffer.Length < 4)
                    continue;

                var actionId = BinaryPrimitives.ReadInt32BigEndian(result.Buffer);

                IUdpTrackerReceivePacket? receivedPacket = actionId switch
                {
                    0 => UdpTrackerConnectResponse.From(result.Buffer),
                    1 => UdpTrackerResponse.From(
                        result.Buffer,
                        result.RemoteEndPoint.Address.IsIPv4MappedToIPv6
                            ? AddressFamily.InterNetwork
                            : AddressFamily.InterNetworkV6
                    ),
                    3 => UdpTrackerErrorResponse.From(result.Buffer),
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
                    packet.Response.TrySetResult(receivedPacket);
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
                    if (transaction.RetryCount >= MAX_RETRIES)
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
                    await udpClient.SendAsync(
                        payload.Memory,
                        transaction.Packet.IPEndPoint,
                        cancellationToken
                    );
                    transaction.RetryCount++;
                    var seconds = 15 * (transaction.RetryCount + 1);
                    transaction.NextRetryTime = DateTime.UtcNow + seconds.Seconds;
                }
            }

            await Task.Delay(1.Seconds, cancellationToken);
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
            );
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
        out bool isNew
    )
    {
        var transaction = new TrackerTransaction(
            packet,
            new TaskCompletionSource<IUdpTrackerReceivePacket>(
                TaskCreationOptions.RunContinuationsAsynchronously
            ),
            trackerId
        );

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
        var transaction = RegisterOrGetTransaction(packet, trackerId, out var isNew);

        if (!isNew)
        {
            return (T)await transaction.Response.Task;
        }

        using var payload = packet.ToMemoryRented();
        var seconds = 15 * (transaction.RetryCount + 1);
        transaction.NextRetryTime = DateTime.UtcNow + seconds.Seconds;

        await udpClient.SendAsync(payload.Memory, packet.IPEndPoint, cancellationToken);

        return (T)await transaction.Response.Task;
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
        );
        _connectionIdByTracker[trackerId] = connectResponse.ConnectionId;
        return connectResponse;
    }

    public void Dispose()
    {
        _packetsByTransactionId.Clear();
        _connectionCreationById.Clear();
        _connectionIdByTracker.Clear();
    }
}
