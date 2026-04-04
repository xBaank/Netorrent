using System.Net;
using System.Threading.Channels;
using Microsoft.Extensions.Logging.Abstractions;
using Netorrent.Extensions;
using Netorrent.P2P.Messages;
using Netorrent.Tests.Fakes;
using Netorrent.TorrentFile.Options;
using Netorrent.Tracker;
using Shouldly;

namespace Netorrent.Tests.Tracker;

[Timeout(10_000)]
public class ScrapeTests
{
    private static IPEndPoint[] DefaultPeers =>
        [
            new IPEndPoint(IPAddress.Parse("127.0.0.1"), 6881),
            new IPEndPoint(IPAddress.Parse("127.0.0.2"), 6882),
        ];

    private static TrackerClient CreateTrackerClient(
        TrackerHandlers handlers,
        string announceUrl = "http://tracker.example.com/announce"
    ) =>
        new TrackerClient(
            new Bitfield(5),
            handlers,
            UsedTrackers.Http | UsedTrackers.Udp,
            6881,
            new(3),
            new(),
            Channel.CreateUnbounded<IPEndPoint>().Writer,
            [
                [announceUrl],
            ],
            new byte[20],
            NullLogger.Instance
        );

    [Test]
    public async Task Should_Get_Scrape_From_Http_Tracker(CancellationToken cancellationToken)
    {
        var fake = new FakeHttpTrackerHandler(DefaultPeers, 1.Seconds);
        var handlers = new TrackerHandlers(fake, null, null, null);
        var client = CreateTrackerClient(handlers);

        var result = await client.ScrapeAsync(cancellationToken);

        result.ShouldNotBeNull();
        result.Seeders.ShouldBe(DefaultPeers.Length);
        result.Leechers.ShouldBe(0);
        result.Downloaded.ShouldBe(0);
    }

    [Test]
    public async Task Should_Get_Scrape_From_Udp_Tracker(CancellationToken cancellationToken)
    {
        await using var fake = new FakeUdpTrackerTransactionManager(DefaultPeers, 1.Seconds);
        var handlers = new TrackerHandlers(null, fake, null, null);
        var client = CreateTrackerClient(handlers, "udp://127.0.0.1:6969/announce");

        var result = await client.ScrapeAsync(cancellationToken);

        result.ShouldNotBeNull();
        result.Seeders.ShouldBe(DefaultPeers.Length);
    }

    [Test]
    public async Task Should_Return_Null_When_Http_Tracker_Scrape_Fails(
        CancellationToken cancellationToken
    )
    {
        var fake = new FakeHttpTrackerHandler(
            DefaultPeers,
            1.Seconds,
            new Exception("scrape error")
        );
        var handlers = new TrackerHandlers(fake, null, null, null);
        var client = CreateTrackerClient(handlers);

        var result = await client.ScrapeAsync(cancellationToken);

        result.ShouldBeNull();
    }

    [Test]
    public async Task Should_Return_Null_When_Udp_Tracker_Scrape_Fails(
        CancellationToken cancellationToken
    )
    {
        await using var fake = new FakeUdpTrackerTransactionManager(
            DefaultPeers,
            1.Seconds,
            new Exception("scrape error")
        );
        var handlers = new TrackerHandlers(null, fake, null, null);
        var client = CreateTrackerClient(handlers, "udp://127.0.0.1:6969/announce");

        var result = await client.ScrapeAsync(cancellationToken);

        result.ShouldBeNull();
    }
}
