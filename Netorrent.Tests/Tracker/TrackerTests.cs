using System.Net;
using System.Threading.Channels;
using Microsoft.Extensions.Logging.Abstractions;
using Netorrent.Extensions;
using Netorrent.Tests.Fakes;
using Netorrent.Tracker;
using Netorrent.Tracker.Http;
using Netorrent.Tracker.Udp;
using Shouldly;

namespace Netorrent.Tests.Tracker;

[Timeout(5_000)]
public class TrackerTests
{
    [Test]
    public async Task Should_get_peers_from_udp_tracker(CancellationToken cancellationToken)
    {
        var interval = 1.Seconds;
        IPEndPoint[] ips =
        [
            new IPEndPoint(IPAddress.Parse("127.0.0.1"), 6881),
            new IPEndPoint(IPAddress.Parse("127.0.0.2"), 6882),
            new IPEndPoint(IPAddress.Parse("::1"), 6881),
            new IPEndPoint(IPAddress.Parse("2001:db8::1"), 6882),
        ];
        var channel = Channel.CreateUnbounded<IPEndPoint>();
        var logger = NullLogger.Instance;
        await using var udptrackerManager = new FakeUdpTrackerTransactionManager(ips, interval);
        await using var udptracker = new UdpTracker(
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
            .Take(ips.Length * 2)
            .ToArrayAsync(cancellationToken);
        cts.Cancel();

        IPEndPoint[] resultIps = [.. ips, .. ips];
        ipendpoints.Length.ShouldBe(ips.Length * 2);
        ipendpoints.ShouldBeEquivalentTo(resultIps);
        await trackerTask.ShouldThrowAsync<OperationCanceledException>();
    }

    [Test]
    public async Task Should_get_peers_from_http_tracker(CancellationToken cancellationToken)
    {
        var interval = 1.Seconds;
        IPEndPoint[] ips =
        [
            new IPEndPoint(IPAddress.Parse("127.0.0.1"), 6881),
            new IPEndPoint(IPAddress.Parse("127.0.0.2"), 6882),
            new IPEndPoint(IPAddress.Parse("::1"), 6881),
            new IPEndPoint(IPAddress.Parse("2001:db8::1"), 6882),
        ];
        var channel = Channel.CreateUnbounded<IPEndPoint>();
        var logger = NullLogger.Instance;
        using var httpClient = new HttpClient();
        var httpTracker = new HttpTracker(
            1,
            new Statistics.TransferStatistics(3),
            new FakeHttpTrackerHandler(ips, interval),
            new(),
            [1, 2, 3],
            "null",
            logger,
            channel,
            null
        );

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var trackerTask = httpTracker.StartAsync(cts.Token).AsTask();
        var ipendpoints = await channel
            .Reader.ReadAllAsync(cancellationToken)
            .Take(ips.Length * 2)
            .ToArrayAsync(cancellationToken: cancellationToken);
        cts.Cancel();

        IPEndPoint[] resultIps = [.. ips, .. ips];
        ipendpoints.Length.ShouldBe(ips.Length * 2);
        ipendpoints.ShouldBeEquivalentTo(resultIps);
        await trackerTask.ShouldThrowAsync<OperationCanceledException>();
    }

    [Test]
    public async Task Should_get_peers_from_tracker_client(CancellationToken cancellationToken)
    {
        var interval = 1.Seconds;
        IPEndPoint[] ips =
        [
            new IPEndPoint(IPAddress.Parse("127.0.0.1"), 6881),
            new IPEndPoint(IPAddress.Parse("127.0.0.2"), 6882),
            new IPEndPoint(IPAddress.Parse("::1"), 6881),
            new IPEndPoint(IPAddress.Parse("2001:db8::1"), 6882),
        ];
        var channel = Channel.CreateUnbounded<IPEndPoint>();
        var logger = NullLogger.Instance;
        await using var udptrackerManager = new FakeUdpTrackerTransactionManager(ips, interval);
        var httpTrackerHandler = new FakeHttpTrackerHandler(ips, interval);

        var trackerClient = new TrackerClient(
            httpTrackerHandler,
            udptrackerManager,
            1,
            new(3),
            new(),
            channel,
            [
                "udp://localhost:1",
                "https://localhost:2",
                "http://localhost:3",
                "aaaa://localhost:4",
            ],
            [1, 2, 3],
            logger,
            null
        );

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var trackerTask = trackerClient.StartAsync(cts.Token);
        var ipendpoints = await channel
            .Reader.ReadAllAsync(cancellationToken)
            .Take(ips.Length * 4) // 2 for udp ipv4 and ipv6 and 2 for http and https
            .ToArrayAsync(cancellationToken: cancellationToken);

        cts.Cancel();

        IPEndPoint[] resultIps = [.. ips, .. ips, .. ips, .. ips];
        ipendpoints.Length.ShouldBe(ips.Length * 4);
        ipendpoints.ShouldBeEquivalentTo(resultIps);
        await trackerTask.ShouldThrowAsync<OperationCanceledException>();
    }
}
