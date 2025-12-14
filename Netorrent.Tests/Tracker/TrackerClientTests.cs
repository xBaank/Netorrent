using System.Net;
using System.Threading.Channels;
using Microsoft.Extensions.Logging.Abstractions;
using Netorrent.Extensions;
using Netorrent.Tests.Fakes;
using Netorrent.Tracker.Udp;
using Netorrent.Tracker.Udp.Response;
using Shouldly;

namespace Netorrent.Tests.Tracker;

public class TrackerClientTests()
{
    [Test]
    [Timeout(5_000)]
    public async Task Should_get_urls_from_tracker(CancellationToken cancellationToken)
    {
        IPEndPoint[] ips =
        [
            new IPEndPoint(IPAddress.Parse("127.0.0.1"), 6881),
            new IPEndPoint(IPAddress.Parse("127.0.0.2"), 6882),
            new IPEndPoint(IPAddress.Parse("::1"), 6881),
            new IPEndPoint(IPAddress.Parse("2001:db8::1"), 6882),
        ];
        var channel = Channel.CreateUnbounded<IPEndPoint>();
        var logger = NullLogger.Instance;
        await using var udptrackerManager = new FakeUdpTrackerTransactionManager(ips);

        var udptracker = new UdpTracker(
            udptrackerManager,
            1,
            new(3),
            new(),
            channel.Writer,
            [1, 2, 3],
            "null",
            new(IPAddress.Loopback, 1),
            logger,
            null
        );
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var trackerTask = udptracker.StartAsync(cts.Token).AsTask();

        var ipendpoints = await channel
            .Reader.ReadAllAsync(cancellationToken)
            .Take(ips.Length)
            .ToArrayAsync(cancellationToken);
        cts.Cancel();

        ipendpoints.Length.ShouldBe(4);
        ips.ShouldBeEquivalentTo(ipendpoints);
        await trackerTask.ShouldThrowAsync<OperationCanceledException>();
    }
}
