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
using R3;
using Shouldly;

namespace Netorrent.Tests.Tracker;

[Timeout(10_000)]
public class UdpTrackerHandlerTests
{
    [Test]
    [Arguments(AddressFamily.InterNetwork)]
    [Arguments(AddressFamily.InterNetworkV6)]
    public async Task Should_get_udp_response(
        AddressFamily addressFamily,
        CancellationToken cancellationToken
    )
    {
        var bindAddress = addressFamily.BindIp();
        IPEndPoint[] ips =
        [
            new IPEndPoint(IPAddress.Parse("127.0.0.1"), 6881),
            new IPEndPoint(IPAddress.Parse("127.0.0.2"), 6882),
            new IPEndPoint(IPAddress.Parse("::1"), 6881),
            new IPEndPoint(IPAddress.Parse("2001:db8::1"), 6882),
        ];
        var fakeUdp = new FakeUdpClient();
        var logger = NullLogger.Instance;

        await using var manager = new UdpTrackerHandler(
            fakeUdp,
            logger,
            15.Seconds,
            1.Seconds,
            1.Minutes,
            8
        );

        var endpoint = new IPEndPoint(bindAddress, 6969);
        var trackerId = Guid.CreateVersion7();

        var connectTask = manager.ConnectAsync(endpoint, trackerId, cancellationToken);

        byte[] infoHash = new byte[20];
        var connectionId = 0x1122334455667788;
        var sent = fakeUdp.SentPackets[0].Payload;
        var transactionId = BinaryPrimitives.ReadInt32BigEndian(sent.Span[12..]);

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
            [.. ips.Where(i => i.AddressFamily == bindAddress.AddressFamily)]
        );
        fakeUdp.EnqueueIncoming(udpTrackerResponse.ToBytes(bindAddress.AddressFamily), endpoint);

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

        await using var manager = new UdpTrackerHandler(
            fakeUdp,
            logger,
            15.Seconds,
            1.Seconds,
            1.Minutes,
            8
        );

        var endpoint = new IPEndPoint(IPAddress.IPv6Loopback, 6969);
        var trackerId = Guid.CreateVersion7();

        var connectTask = manager.ConnectAsync(endpoint, trackerId, cancellationToken);

        byte[] infoHash = new byte[20];
        var connectionId = 0x1122334455667788;
        var sent = fakeUdp.SentPackets[0].Payload;
        var transactionId = BinaryPrimitives.ReadInt32BigEndian(sent.Span[12..]);

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

        await using var manager = new UdpTrackerHandler(
            fakeUdp,
            logger,
            0.01.Seconds,
            0.01.Seconds,
            1.Minutes,
            8
        );

        var endpoint = new IPEndPoint(IPAddress.IPv6Loopback, 6969);
        var trackerId = Guid.CreateVersion7();

        var connectTask = manager.ConnectAsync(endpoint, trackerId, cancellationToken);

        byte[] infoHash = new byte[20];
        var connectionid = 0x1122334455667788;
        var sent = fakeUdp.SentPackets[0].Payload;
        var transactionId = BinaryPrimitives.ReadInt32BigEndian(sent.Span[12..]);

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

    [Test]
    [Arguments(AddressFamily.InterNetwork)]
    [Arguments(AddressFamily.InterNetworkV6)]
    public async Task Should_reconnect_and_receive_udp_response(
        AddressFamily addressFamily,
        CancellationToken cancellationToken
    )
    {
        var bindAddress = addressFamily.BindIp();
        IPEndPoint[] ips =
        [
            new IPEndPoint(IPAddress.Parse("127.0.0.1"), 6881),
            new IPEndPoint(IPAddress.Parse("127.0.0.2"), 6882),
            new IPEndPoint(IPAddress.Parse("::1"), 6881),
            new IPEndPoint(IPAddress.Parse("2001:db8::1"), 6882),
        ];
        var fakeUdp = new FakeUdpClient();
        var logger = NullLogger.Instance;

        await using var manager = new UdpTrackerHandler(
            fakeUdp,
            logger,
            1.Seconds,
            1.Seconds,
            1.Minutes,
            8
        );

        var endpoint = new IPEndPoint(bindAddress, 6969);
        var trackerId = Guid.CreateVersion7();

        var infoHash = new byte[20];
        var connectionid = 0x1122334455667788;

        var request = new UdpTrackerRequest(
            endpoint,
            infoHash,
            new(),
            0,
            3,
            0,
            Events.Started,
            1,
            Random.Shared.Next(), //Fake connection id
            Random.Shared.Next()
        );

        //Send a request that with no connection id (is outdated) and get the 2 first sent packets (udp request and udp connection request)
        var sentPacketsTask = fakeUdp.OnSent.Take(2).ToListAsync(cancellationToken);
        var sendTask = manager.SendAsync<UdpTrackerResponse>(request, trackerId, cancellationToken);
        var sentPackets = await sentPacketsTask;
        var transactionId = BinaryPrimitives.ReadInt32BigEndian(sentPackets[1].Span[12..]);
        //Set the response to the udp connection request once its made
        var connectResponse = new UdpTrackerConnectResponse(transactionId, connectionid);
        fakeUdp.EnqueueIncoming(connectResponse.ToBytes(), endpoint);
        //Get the actual udp request that is sent again with a valid connection id
        var sent = await fakeUdp.OnSent.FirstAsync(cancellationToken);
        var udpTrackerResponse = new UdpTrackerResponse(
            request.TransactionId,
            10,
            2,
            0,
            [.. ips.Where(i => i.AddressFamily == bindAddress.AddressFamily)]
        );
        fakeUdp.EnqueueIncoming(udpTrackerResponse.ToBytes(bindAddress.AddressFamily), endpoint);
        var result = await sendTask;

        fakeUdp.SentPackets.Count.ShouldBe(3);
        fakeUdp.SentPackets[2].Payload.ShouldBe(sent);
        result.TransactionId.ShouldBe(request.TransactionId);
        result.Interval.ShouldBe(10);
        result.Leechers.ShouldBe(2);
        result.Seeders.ShouldBe(0);
        result.Peers.Count.ShouldBe(2);
    }
}
