using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
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
            "null",
            new(IPAddress.Loopback, 1),
            ctx.Logger
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
            ctx.Logger,
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
            ctx.Logger,
            ctx.Channel
        );

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var trackerTask = httpTracker.StartAsync(cts.Token).AsTask();

        await Task.Delay(5.Seconds, cancellationToken);
        cts.Cancel();
        ctx.Channel.Writer.TryComplete();

        var ipendpoints = await ctx
            .Channel.Reader.ReadAllAsync(cancellationToken)
            .ToArrayAsync(cancellationToken: cancellationToken)
            .AsTask();

        ipendpoints.Length.ShouldBe(0);
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
            "null",
            new(IPAddress.Loopback, 1),
            ctx.Logger
        );

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var trackerTask = udptracker.StartAsync(cts.Token).AsTask();

        await Task.Delay(5.Seconds, cancellationToken);
        cts.Cancel();
        ctx.Channel.Writer.TryComplete();

        var ipendpoints = await ctx
            .Channel.Reader.ReadAllAsync(cancellationToken)
            .ToArrayAsync(cancellationToken: cancellationToken)
            .AsTask();

        ipendpoints.Length.ShouldBe(0);
    }

    [Test]
    public async Task Should_get_peers_from_tracker_client(CancellationToken cancellationToken)
    {
        var ctx = CreateDefaultContext();

        await using var udptrackerManager = new FakeUdpTrackerTransactionManager(
            ctx.Ips,
            ctx.Interval
        );

        var httpTrackerHandler = new FakeHttpTrackerHandler(ctx.Ips, ctx.Interval);

        var trackerHandlers = new TrackerHandlers(
            httpTrackerHandler,
            udptrackerManager,
            httpTrackerHandler,
            udptrackerManager
        );

        await using var trackerClient = new TrackerClient(
            trackerHandlers,
            UsedTrackers.Http | UsedTrackers.Udp,
            1,
            new(3),
            new(),
            ctx.Channel,
            [
                "udp://localhost:1",
                "https://localhost:2",
                "http://localhost:3",
                "aaaa://localhost:4",
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
        ipendpoints.ShouldBe(resultIps);
    }
}
