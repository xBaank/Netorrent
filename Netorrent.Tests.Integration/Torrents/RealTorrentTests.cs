using Microsoft.Extensions.Logging;
using Netorrent.Dht;
using Netorrent.TorrentFile;
using Shouldly;

namespace Netorrent.Tests.Integration.Torrents;

[Timeout(8 * 60_000)]
public class RealTorrentTests
{
    private static ILogger Logger => new TUnitLogger(TestContext.Current!.GetDefaultLogger());

    [Test]
    public async Task Should_Download_Real_Torrent(CancellationToken cancellationToken)
    {
        await using var torrentClient = new TorrentClient(o => o with { Logger = Logger });
        await using var torrent = await torrentClient.LoadTorrentAsync(
            "Data/debian-13.3.0-amd64-netinst.iso.torrent",
            "Output",
            cancellationToken: cancellationToken
        );
        cancellationToken.Register(torrent.Stop);
        await torrent.StartAsync();
        await torrent.Completion;
    }

    [Test]
    public async Task Should_Download_Real_Torrent_Via_Dht_Only(CancellationToken cancellationToken)
    {
        await using var torrentClient = new TorrentClient(o =>
            o with
            {
                Logger = Logger,
                UsedTrackers = 0, // disable all trackers — DHT only
                DhtOptions = DhtClientOptions.Default,
            }
        );
        await using var torrent = await torrentClient.LoadTorrentAsync(
            "Data/debian-13.3.0-amd64-netinst.iso.torrent",
            "Output/DhtOnly",
            cancellationToken: cancellationToken
        );
        cancellationToken.Register(torrent.Stop);
        await torrent.StartAsync();
        await torrent.Completion;
    }

    [Test]
    public async Task Should_Scrape_Real_Torrent(CancellationToken cancellationToken)
    {
        await using var torrentClient = new TorrentClient(o => o with { Logger = Logger });
        await using var torrent = await torrentClient.LoadTorrentAsync(
            "Data/debian-13.3.0-amd64-netinst.iso.torrent",
            "Output",
            cancellationToken: cancellationToken
        );

        var result = await torrent.ScrapeAsync(cancellationToken);

        result.ShouldNotBeNull();
        result.Seeders.ShouldBeGreaterThan(0);
    }
}
