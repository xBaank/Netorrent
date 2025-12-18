using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging.Abstractions;
using Netorrent.Extensions;
using Netorrent.Tests.Extensions;
using Netorrent.Tracker;
using Netorrent.Tracker.Udp;
using Netorrent.Tracker.Udp.Exceptions;
using Netorrent.Tracker.Udp.Request;
using Netorrent.Tracker.Udp.Response;
using Shouldly;

namespace Netorrent.Tests.Tracker;

//TODO  Test reconnect
[Timeout(20_000)]
public class UdpTrackerTransactionManagerTests
{
    [Test]
    [Arguments(AddressFamily.InterNetwork)]
    [Arguments(AddressFamily.InterNetworkV6)]
    public async Task Should_get_udp_response(
        AddressFamily addressFamily,
        CancellationToken cancellationToken
    )
    {
        IPEndPoint[] ips =
        [
            new IPEndPoint(IPAddress.Parse("127.0.0.1"), 6881),
            new IPEndPoint(IPAddress.Parse("127.0.0.2"), 6882),
            new IPEndPoint(IPAddress.Parse("::1"), 6881),
            new IPEndPoint(IPAddress.Parse("2001:db8::1"), 6882),
        ];
        var fakeUdp = new FakeUdpClient();
        var logger = NullLogger.Instance;

        await using var manager = new UdpTrackerTransactionManager(fakeUdp, logger, 15.Seconds, 8);
        manager.Start();

        var endpoint = new IPEndPoint(
            addressFamily == AddressFamily.InterNetwork
                ? IPAddress.Loopback
                : IPAddress.IPv6Loopback,
            6969
        );
        var trackerId = Guid.CreateVersion7();

        var connectTask = manager.ConnectAsync(endpoint, trackerId, cancellationToken);

        byte[] infoHash = new byte[20];
        var connectionId = 0x1122334455667788;
        var sent = fakeUdp.SentPackets[0].Payload;
        var transactionId = BinaryPrimitives.ReadInt32BigEndian(sent.AsSpan(12));

        var connectResponse = new UdpTrackerConnectResponse(transactionId, connectionId);

        fakeUdp.EnqueueIncoming(connectResponse.ToBytes(), endpoint);

        var conenctResult = await connectTask;

        var request = new UdpTrackerRequest(
            endpoint,
            infoHash,
            new(),
            0,
            3,
            0,
            Events.Started,
            1,
            conenctResult.ConnectionId,
            Random.Shared.Next()
        );

        var sendTask = manager.SendAsync<UdpTrackerResponse>(request, trackerId, cancellationToken);

        var udpTrackerResponse = new UdpTrackerResponse(
            request.TransactionId,
            10,
            2,
            0,
            [.. ips.Where(i => i.AddressFamily == addressFamily)]
        );
        fakeUdp.EnqueueIncoming(udpTrackerResponse.ToBytes(addressFamily), endpoint);

        var result = await sendTask;

        conenctResult.TransactionId.ShouldBe(transactionId);
        conenctResult.ConnectionId.ShouldBe(0x1122334455667788);
        result.TransactionId.ShouldBe(request.TransactionId);
        result.Interval.ShouldBe(10);
        result.Leechers.ShouldBe(2);
        result.Seeders.ShouldBe(0);
        result.Peers.Count.ShouldBe(2);
    }

    [Test]
    public async Task Should_get_udp_error_response(CancellationToken cancellationToken)
    {
        var fakeUdp = new FakeUdpClient();
        var logger = NullLogger.Instance;

        await using var manager = new UdpTrackerTransactionManager(fakeUdp, logger, 15.Seconds, 8);
        manager.Start();

        var endpoint = new IPEndPoint(IPAddress.IPv6Loopback, 6969);
        var trackerId = Guid.CreateVersion7();

        var connectTask = manager.ConnectAsync(endpoint, trackerId, cancellationToken);

        byte[] infoHash = new byte[20];
        var connectionId = 0x1122334455667788;
        var sent = fakeUdp.SentPackets[0].Payload;
        var transactionId = BinaryPrimitives.ReadInt32BigEndian(sent.AsSpan(12));

        var connectResponse = new UdpTrackerConnectResponse(transactionId, connectionId);

        fakeUdp.EnqueueIncoming(connectResponse.ToBytes(), endpoint);

        var conenctResult = await connectTask;

        var request = new UdpTrackerRequest(
            endpoint,
            infoHash,
            new(),
            0,
            3,
            0,
            Events.Started,
            1,
            conenctResult.ConnectionId,
            Random.Shared.Next()
        );

        var sendTask = manager.SendAsync<UdpTrackerResponse>(request, trackerId, cancellationToken);

        var udpTrackerErrorResponse = new UdpTrackerErrorResponse(
            request.TransactionId,
            "Test error"
        );
        fakeUdp.EnqueueIncoming(udpTrackerErrorResponse.ToBytes(), endpoint);

        conenctResult.TransactionId.ShouldBe(transactionId);
        conenctResult.ConnectionId.ShouldBe(connectionId);
        await sendTask.ShouldThrowAsync<UdpTrackerException>();
    }

    [Test]
    public async Task Should_get_udp_timeout_response(CancellationToken cancellationToken)
    {
        var fakeUdp = new FakeUdpClient();
        var logger = NullLogger.Instance;

        await using var manager = new UdpTrackerTransactionManager(
            fakeUdp,
            logger,
            0.01.Seconds,
            8
        );
        manager.Start();

        var endpoint = new IPEndPoint(IPAddress.IPv6Loopback, 6969);
        var trackerId = Guid.CreateVersion7();

        var connectTask = manager.ConnectAsync(endpoint, trackerId, cancellationToken);

        byte[] infoHash = new byte[20];
        var connectionid = 0x1122334455667788;
        var sent = fakeUdp.SentPackets[0].Payload;
        var transactionId = BinaryPrimitives.ReadInt32BigEndian(sent.AsSpan(12));

        var connectResponse = new UdpTrackerConnectResponse(transactionId, connectionid);

        fakeUdp.EnqueueIncoming(connectResponse.ToBytes(), endpoint);

        var conenctResult = await connectTask;

        var request = new UdpTrackerRequest(
            endpoint,
            infoHash,
            new(),
            0,
            3,
            0,
            Events.Started,
            1,
            conenctResult.ConnectionId,
            Random.Shared.Next()
        );

        var sendTask = manager.SendAsync<UdpTrackerResponse>(request, trackerId, cancellationToken);

        conenctResult.TransactionId.ShouldBe(transactionId);
        conenctResult.ConnectionId.ShouldBe(connectionid);
        await sendTask.ShouldThrowAsync<TimeoutException>();
    }
}
