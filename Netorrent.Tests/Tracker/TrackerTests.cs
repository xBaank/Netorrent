using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Netorrent.Exceptions;
using Netorrent.Extensions;
using Netorrent.Tests.Extensions;
using Netorrent.Tests.Fakes;
using Netorrent.TorrentFile.Options;
using Netorrent.Tracker;
using Netorrent.Tracker.Http;
using Netorrent.Tracker.Udp;
using Shouldly;

namespace Netorrent.Tests.Tracker;

[Timeout(10_000)]
public class TrackerTests
{
    private record TestContext(
        IPEndPoint[] Ips,
        Channel<IPEndPoint> Channel,
        ILogger Logger,
        TimeSpan Interval
    );

    private static TestContext CreateDefaultContext()
    {
        var interval = 1.Seconds;
        IPEndPoint[] ips =
        [
            new IPEndPoint(IPAddress.Parse("127.0.0.1"), 6881),
            new IPEndPoint(IPAddress.Parse("127.0.0.2"), 6882),
            new IPEndPoint(IPAddress.Parse("::1"), 6881),
            new IPEndPoint(IPAddress.Parse("2001:db8::1"), 6882),
        ];

        return new TestContext(
            Ips: ips,
            Channel: Channel.CreateUnbounded<IPEndPoint>(),
            Logger: NullLogger.Instance,
            Interval: interval
        );
    }

    private static Task<IPEndPoint[]> ReadTakeAsync(
        ChannelReader<IPEndPoint> reader,
        int count,
        CancellationToken ct
    )
    {
        return reader.ReadAllAsync(ct).Take(count).ToArrayAsync(cancellationToken: ct).AsTask();
    }

    [Test]
    public async Task Should_get_peers_from_udp_tracker(CancellationToken cancellationToken)
    {
        var ctx = CreateDefaultContext();

        await using var udptrackerManager = new FakeUdpTrackerTransactionManager(
            ctx.Ips,
            ctx.Interval
        );

        var udptracker = new UdpTracker(
            udptrackerManager,
            1,
            new(3),
            new(),
            ctx.Channel.Writer,
            new byte[20],
            new(IPAddress.Loopback, 1)
        );

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var trackerTask = udptracker.StartAsync(cts.Token).AsTask();

        var ipendpoints = await ReadTakeAsync(
            ctx.Channel.Reader,
            ctx.Ips.Length * 2,
            cancellationToken
        );

        cts.Cancel();

        IPEndPoint[] resultIps = [.. ctx.Ips, .. ctx.Ips];
        ipendpoints.Length.ShouldBe(ctx.Ips.Length * 2);
        ipendpoints.ShouldBeEquivalentTo(resultIps);
    }

    [Test]
    public async Task Should_get_peers_from_http_tracker(CancellationToken cancellationToken)
    {
        var ctx = CreateDefaultContext();

        var httpTracker = new HttpTracker(
            1,
            new Statistics.DataStatistics(3),
            new FakeHttpTrackerHandler(ctx.Ips, ctx.Interval),
            new(),
            new byte[20],
            "null",
            ctx.Channel
        );

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var trackerTask = httpTracker.StartAsync(cts.Token).AsTask();

        var ipendpoints = await ReadTakeAsync(
            ctx.Channel.Reader,
            ctx.Ips.Length * 2,
            cancellationToken
        );

        cts.Cancel();

        IPEndPoint[] resultIps = [.. ctx.Ips, .. ctx.Ips];
        ipendpoints.Length.ShouldBe(ctx.Ips.Length * 2);
        ipendpoints.ShouldBeEquivalentTo(resultIps);
    }

    [Test]
    public async Task Should_not_get_peers_from_http_tracker(CancellationToken cancellationToken)
    {
        var ctx = CreateDefaultContext();

        var httpTracker = new HttpTracker(
            1,
            new Statistics.DataStatistics(3),
            new FakeHttpTrackerHandler(ctx.Ips, ctx.Interval, new Exception()),
            new(),
            new byte[20],
            "null",
            ctx.Channel
        );

        await httpTracker
            .StartAsync(cancellationToken)
            .AsTask()
            .ShouldThrowAsync<AnnounceException>();
    }

    [Test]
    public async Task Should_not_get_peers_from_udp_tracker(CancellationToken cancellationToken)
    {
        var ctx = CreateDefaultContext();

        await using var udptrackerManager = new FakeUdpTrackerTransactionManager(
            ctx.Ips,
            ctx.Interval,
            new Exception()
        );

        var udptracker = new UdpTracker(
            udptrackerManager,
            1,
            new(3),
            new(),
            ctx.Channel.Writer,
            new byte[20],
            new(IPAddress.Loopback, 1)
        );

        await udptracker
            .StartAsync(cancellationToken)
            .AsTask()
            .ShouldThrowAsync<AnnounceException>();
    }

    [Test]
    public async Task Should_not_get_peers_from_tracker_client(CancellationToken cancellationToken)
    {
        var ctx = CreateDefaultContext();

        await using var udptrackerManagerv4 = new FakeUdpTrackerTransactionManager(
            [.. ctx.Ips.Where(i => i.AddressFamily == AddressFamily.InterNetwork)],
            ctx.Interval,
            new Exception()
        );

        await using var udptrackerManagerv6 = new FakeUdpTrackerTransactionManager(
            [.. ctx.Ips.Where(i => i.AddressFamily == AddressFamily.InterNetworkV6)],
            ctx.Interval,
            new Exception()
        );

        using var httpTrackerHandlerv4 = new FakeHttpTrackerHandler(
            [.. ctx.Ips.Where(i => i.AddressFamily == AddressFamily.InterNetwork)],
            ctx.Interval,
            new Exception()
        );
        using var httpTrackerHandlerv6 = new FakeHttpTrackerHandler(
            [.. ctx.Ips.Where(i => i.AddressFamily == AddressFamily.InterNetworkV6)],
            ctx.Interval,
            new Exception()
        );

        var trackerHandlers = new TrackerHandlers(
            httpTrackerHandlerv4,
            udptrackerManagerv4,
            httpTrackerHandlerv6,
            udptrackerManagerv6
        );

        await using var trackerClient = new TrackerClient(
            trackerHandlers,
            UsedTrackers.Http | UsedTrackers.Udp,
            1,
            new(3),
            new(),
            ctx.Channel,
            [
                ["udp://localhost:1", "https://localhost:2"],
                ["http://localhost:3", "aaaa://localhost:4"],
            ],
            new byte[20],
            ctx.Logger
        );

        await trackerClient.StartAsync(cancellationToken).ShouldNotThrowAsync();
    }

    [Test]
    public async Task Should_get_peers_from_tracker_client(CancellationToken cancellationToken)
    {
        var ctx = CreateDefaultContext();

        await using var udptrackerManagerv4 = new FakeUdpTrackerTransactionManager(
            [.. ctx.Ips.Where(i => i.AddressFamily == AddressFamily.InterNetwork)],
            ctx.Interval
        );

        await using var udptrackerManagerv6 = new FakeUdpTrackerTransactionManager(
            [.. ctx.Ips.Where(i => i.AddressFamily == AddressFamily.InterNetworkV6)],
            ctx.Interval
        );

        using var httpTrackerHandlerv4 = new FakeHttpTrackerHandler(
            [.. ctx.Ips.Where(i => i.AddressFamily == AddressFamily.InterNetwork)],
            ctx.Interval
        );
        using var httpTrackerHandlerv6 = new FakeHttpTrackerHandler(
            [.. ctx.Ips.Where(i => i.AddressFamily == AddressFamily.InterNetworkV6)],
            ctx.Interval
        );

        var trackerHandlers = new TrackerHandlers(
            httpTrackerHandlerv4,
            udptrackerManagerv4,
            httpTrackerHandlerv6,
            udptrackerManagerv6
        );

        await using var trackerClient = new TrackerClient(
            trackerHandlers,
            UsedTrackers.Http | UsedTrackers.Udp,
            1,
            new(3),
            new(),
            ctx.Channel,
            [
                ["udp://localhost:1", "https://localhost:2"],
                ["http://localhost:3", "aaaa://localhost:4"],
            ],
            new byte[20],
            ctx.Logger
        );

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var trackerTask = trackerClient.StartAsync(cts.Token);
        var ipendpoints = await ReadTakeAsync(
            ctx.Channel.Reader,
            ctx.Ips.Length * 4,
            cancellationToken
        );

        cts.Cancel();

        IPEndPoint[] resultIps = [.. ctx.Ips, .. ctx.Ips, .. ctx.Ips, .. ctx.Ips];
        ipendpoints.Length.ShouldBe(ctx.Ips.Length * 4);
        var sorted = ipendpoints.OrderBy(i => i.ToString()).ToArray();
        sorted.ShouldBe([.. resultIps.OrderBy(i => i.ToString())]);
    }
}
