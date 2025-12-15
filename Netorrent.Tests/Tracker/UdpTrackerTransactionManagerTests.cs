using System.Buffers.Binary;
using System.Net;
using Microsoft.Extensions.Logging.Abstractions;
using Netorrent.Extensions;
using Netorrent.Tracker.Udp;
using Shouldly;

namespace Netorrent.Tests.Tracker;

[Timeout(10_000)]
public class UdpTrackerTransactionManagerTests
{
    [Test]
    public async Task Should_connect_and_get_connection_id(CancellationToken cancellationToken)
    {
        var fakeUdp = new FakeUdpClient();
        var logger = NullLogger.Instance;

        await using var manager = new UdpTrackerTransactionManager(fakeUdp, logger);
        manager.Start();

        var endpoint = new IPEndPoint(IPAddress.Loopback, 6969);
        var trackerId = Guid.NewGuid();

        var connectTask = manager.ConnectAsync(endpoint, trackerId, cancellationToken);

        var sent = fakeUdp.SentPackets[0].Payload;
        var transactionId = BinaryPrimitives.ReadInt32BigEndian(sent.AsSpan(12));
        var response = new byte[16];
        BinaryPrimitives.WriteInt32BigEndian(response.AsSpan(0), 0); // action = connect
        BinaryPrimitives.WriteInt32BigEndian(response.AsSpan(4), transactionId);
        BinaryPrimitives.WriteInt64BigEndian(response.AsSpan(8), 0x1122334455667788);

        fakeUdp.EnqueueIncoming(response, endpoint);

        var result = await connectTask;

        result.Action.ShouldBe(0);
        result.TransactionId.ShouldBe(transactionId);
        result.ConnectionId.ShouldBe(0x1122334455667788);
    }
}
