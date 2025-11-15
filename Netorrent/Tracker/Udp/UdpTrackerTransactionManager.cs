using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Netorrent.Tracker.Udp.Request;
using Netorrent.Tracker.Udp.Response;
using TimeSpanXt;

namespace Netorrent.Tracker.Udp;

internal record TrackerTransaction(
    IUdpTrackerSendPacket Packet,
    TaskCompletionSource<IUdpTrackerReceivePacket> Response,
    DateTime CreatedAt
)
{
    public int RetryCount { get; set; }
    public DateTime? NextRetryTime { get; set; }
};

internal class UdpTrackerTransactionManager(UdpClient udpClient, ILogger logger) : IDisposable
{
    private const int MAX_RETRIES = 8;
    private readonly ConcurrentDictionary<int, TrackerTransaction> _packetsByTransaction = [];
    private readonly ConcurrentDictionary<long, DateTime> _connectionIdsByCreation = [];

    private CancellationTokenSource _cancellationTokenSource = new();
    public Task? TrackerManagerTask { get; private set; }

    public void Start(CancellationToken cancellationToken)
    {
        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken
        );
        TrackerManagerTask = Task.WhenAll(
            ReceiveLoopAsync(_cancellationTokenSource.Token),
            RetryLoopAsync(_cancellationTokenSource.Token)
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
                    _connectionIdsByCreation[udpTrackerConnectResponse.ConnectionId] =
                        DateTime.UtcNow;
                }

                if (_packetsByTransaction.TryGetValue(receivedPacket.TransactionId, out var packet))
                {
                    packet.Response.TrySetResult(receivedPacket);
                    _packetsByTransaction.TryRemove(receivedPacket.TransactionId, out _);
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
            var now = DateTime.UtcNow;
            foreach (var transaction in _packetsByTransaction.Values)
            {
                if (now > transaction.NextRetryTime)
                {
                    if (transaction.RetryCount >= MAX_RETRIES)
                    {
                        transaction.Response.TrySetException(
                            new TimeoutException("Tracker did not respond")
                        );
                        _packetsByTransaction.TryRemove(transaction.Packet.TransactionId, out _);
                        continue;
                    }

                    using var payload = transaction.Packet.ToMemoryRented();
                    await udpClient.SendAsync(
                        payload.Memory,
                        transaction.Packet.IPEndPoint,
                        cancellationToken
                    );
                    transaction.RetryCount++;
                    var seconds = 15 * (transaction.RetryCount + 1);
                    transaction.NextRetryTime = DateTime.UtcNow + seconds.Seconds();
                }
            }

            await Task.Delay(1.Seconds(), cancellationToken);
        }
    }

    private TrackerTransaction RegisterOrGetTransaction(
        IUdpTrackerSendPacket packet,
        out bool isNew
    )
    {
        var transaction = new TrackerTransaction(
            packet,
            new TaskCompletionSource<IUdpTrackerReceivePacket>(
                TaskCreationOptions.RunContinuationsAsynchronously
            ),
            DateTime.UtcNow
        );

        if (_packetsByTransaction.TryAdd(packet.TransactionId, transaction))
        {
            isNew = true;
            return transaction;
        }

        _packetsByTransaction.TryGetValue(packet.TransactionId, out var existing);
        isNew = false;
        return existing!;
    }

    public async Task<T> SendAsync<T>(
        IUdpTrackerSendPacket packet,
        CancellationToken cancellationToken
    )
        where T : IUdpTrackerReceivePacket
    {
        if (packet is UdpTrackerRequest trackerRequest)
        {
            if (!_connectionIdsByCreation.ContainsKey(packet.TransactionId))
            {
                var response = await ConnectAsync(packet.IPEndPoint, cancellationToken);
                trackerRequest = trackerRequest with { ConnectionId = response.ConnectionId };
            }
            else if (
                _connectionIdsByCreation.TryGetValue(
                    trackerRequest.ConnectionId,
                    out var creationTime
                )
            )
            {
                var diff = DateTime.UtcNow - creationTime;
                if (diff > 1.Minutes())
                {
                    var response = await ConnectAsync(packet.IPEndPoint, cancellationToken);
                    trackerRequest = trackerRequest with { ConnectionId = response.ConnectionId };
                }
            }

            packet = trackerRequest with { TransactionId = MakeTransactionId() };
        }

        var transaction = RegisterOrGetTransaction(packet, out var isNew);

        if (!isNew)
        {
            return (T)await transaction.Response.Task;
        }

        using var payload = packet.ToMemoryRented();
        var seconds = 15 * (transaction.RetryCount + 1);
        transaction.NextRetryTime = DateTime.UtcNow + seconds.Seconds();

        await udpClient.SendAsync(payload.Memory, packet.IPEndPoint, cancellationToken);

        return (T)await transaction.Response.Task;
    }

    public int MakeTransactionId()
    {
        int id;
        do
        {
            id = Random.Shared.Next();
        } while (_packetsByTransaction.ContainsKey(id));
        return id;
    }

    //TODO delegate connect to the tracker
    public async Task<UdpTrackerConnectResponse> ConnectAsync(
        IPEndPoint endPoint,
        CancellationToken cancellationToken
    )
    {
        var transactionId = MakeTransactionId();
        return await SendAsync<UdpTrackerConnectResponse>(
            new UdpTrackerConnectRequest(endPoint, transactionId),
            cancellationToken
        );
    }

    public void Dispose()
    {
        _packetsByTransaction.Clear();
        _connectionIdsByCreation.Clear();
        _cancellationTokenSource.Cancel();
    }
}
