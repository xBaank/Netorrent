using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.Tracker.Udp.Request;
using Netorrent.Tracker.Udp.Response;
using TimeSpanXt;

namespace Netorrent.Tracker.Udp;

internal record TrackerTransaction(
    int TransactionId,
    IPEndPoint Endpoint,
    TaskCompletionSource<IUdpTrackerReceivePacket> Response,
    DateTime CreatedAt
);

internal class UdpTrackerTransactionManager(UdpClient udpClient, ILogger logger)
{
    private readonly ConcurrentDictionary<int, TrackerTransaction> _packetsByTransaction = [];
    private readonly ConcurrentDictionary<long, DateTime> _connectionIdsByCreation = [];
    private readonly Channel<IUdpTrackerSendPacket> _sendPacketsChannel =
        Channel.CreateBounded<IUdpTrackerSendPacket>(
            new BoundedChannelOptions(100) { SingleReader = true, SingleWriter = false }
        );

    public void Start(CancellationToken cancellationToken)
    {
        SendLoopAsync(cancellationToken);
        ReceiveLoopAsync(cancellationToken);
    }

    private async Task SendLoopAsync(CancellationToken cancellationToken)
    {
        await foreach (var packet in _sendPacketsChannel.Reader.ReadAllAsync(cancellationToken))
        {
            if (packet is UdpTrackerRequest udpTrackerRequest)
            {
                var connectionIdCreatedAt = _connectionIdsByCreation[
                    udpTrackerRequest.ConnectionId
                ];
                var passedTime = DateTime.UtcNow - connectionIdCreatedAt;
                if (passedTime > 1.Minutes())
                {
                    try
                    {
                        var response = await ConnectAsync(packet.IPEndPoint, cancellationToken);
                        var newPacket = udpTrackerRequest with
                        {
                            ConnectionId = response.ConnectionId,
                        };
                        await _sendPacketsChannel.Writer.WriteAsync(newPacket, cancellationToken);
                        continue;
                    }
                    catch (OperationCanceledException ex)
                    {
                        if (
                            _packetsByTransaction.TryGetValue(
                                packet.TransactionId,
                                out var trackerTransaction
                            )
                        )
                            trackerTransaction.Response.SetException(ex);
                    }
                }
            }
            using var payload = packet.To();
            await udpClient.SendAsync(payload.Memory, packet.IPEndPoint, cancellationToken);
        }
    }

    private async Task ReceiveLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var result = await udpClient.ReceiveAsync(cancellationToken);
            var actionId = BinaryPrimitives.ReadInt32BigEndian(result.Buffer);

            IUdpTrackerReceivePacket? receivedPacket = actionId switch
            {
                0 => UdpTrackerConnectResponse.From(result.Buffer),
                1 => UdpTrackerResponse.From(result.Buffer, result.RemoteEndPoint.AddressFamily),
                3 => UdpTrackerErrorResponse.From(result.Buffer),
                _ => null,
            };

            if (receivedPacket is null)
                continue;

            if (receivedPacket is UdpTrackerConnectResponse udpTrackerConnectResponse)
            {
                _connectionIdsByCreation[udpTrackerConnectResponse.ConnectionId] = DateTime.UtcNow;
            }

            if (_packetsByTransaction.TryGetValue(receivedPacket.TransactionId, out var packet))
            {
                packet.Response.SetResult(receivedPacket);
                _packetsByTransaction.TryRemove(receivedPacket.TransactionId, out _);
            }
        }
    }

    private TrackerTransaction RegisterOrGetTransaction(
        IUdpTrackerSendPacket packet,
        out bool isNew
    )
    {
        var transaction = new TrackerTransaction(
            packet.TransactionId,
            packet.IPEndPoint,
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

    private async Task<T> SendInternalAsync<T>(
        IUdpTrackerSendPacket packet,
        Func<IUdpTrackerSendPacket, CancellationToken, Task> sendOperation,
        CancellationToken cancellationToken
    )
        where T : IUdpTrackerReceivePacket
    {
        if (_packetsByTransaction.TryGetValue(packet.TransactionId, out var existingTxn))
        {
            var existingResult = await existingTxn.Response.Task.ConfigureAwait(false);
            return (T)existingResult;
        }

        var txn = RegisterOrGetTransaction(packet, out bool isNew);

        if (!isNew)
        {
            var racedResult = await txn.Response.Task.ConfigureAwait(false);
            return (T)racedResult;
        }

        try
        {
            await sendOperation(packet, cancellationToken).ConfigureAwait(false);

            var response = await txn.Response.Task.ConfigureAwait(false);
            return (T)response;
        }
        catch
        {
            _packetsByTransaction.TryRemove(packet.TransactionId, out _);
            throw;
        }
    }

    public Task<T> SendAsync<T>(IUdpTrackerSendPacket packet, CancellationToken cancellationToken)
        where T : IUdpTrackerReceivePacket
    {
        Task sendOp(IUdpTrackerSendPacket packet, CancellationToken cancellationToken) =>
            _sendPacketsChannel.Writer.WriteAsync(packet, cancellationToken).AsTask();

        return SendInternalAsync<T>(packet, sendOp, cancellationToken);
    }

    public Task<T> SendDirectlyAsync<T>(
        IUdpTrackerSendPacket packet,
        CancellationToken cancellationToken
    )
        where T : IUdpTrackerReceivePacket
    {
        async Task sendOp(IUdpTrackerSendPacket packet, CancellationToken cancellationToken)
        {
            using var payload = packet.To();
            await udpClient.SendAsync(payload.Memory, cancellationToken).ConfigureAwait(false);
        }

        return SendInternalAsync<T>(packet, sendOp, cancellationToken);
    }

    private async Task<UdpTrackerConnectResponse> ConnectAsync(
        IPEndPoint endPoint,
        CancellationToken cancellationToken
    )
    {
        try
        {
            using var cts = cancellationToken.WithTimeout(1.Minutes());
            var transactionId = Random.Shared.Next();
            return await SendDirectlyAsync<UdpTrackerConnectResponse>(
                new UdpTrackerConnectRequest(endPoint, transactionId),
                cts.Token
            );
        }
        catch (OperationCanceledException ex)
        {
            logger.LogDebug(ex, "Couldn't connect to {ip}", endPoint);
            throw;
        }
    }
}
