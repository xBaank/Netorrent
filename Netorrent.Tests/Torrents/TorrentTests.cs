using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.Tests.Fixtures;
using Netorrent.TorrentFile;
using Shouldly;
using TimeSpanXt;

namespace Netorrent.Tests.Torrents;

public enum AnnounceType
{
    Http,
    Udp,
}

public class TorrentTests(OpenTrackerFixture fixture, ITestOutputHelper outputHelper)
    : IClassFixture<OpenTrackerFixture>
{
    private readonly OpenTrackerFixture _fixture = fixture;

    [Theory]
    [InlineData(AnnounceType.Http)]
    [InlineData(AnnounceType.Udp)]
    public async Task Should_download_torrent(AnnounceType announceType)
    {
        var loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Trace)
        );
        var announceUrl = announceType switch
        {
            AnnounceType.Http => _fixture.AnnounceUrl,
            AnnounceType.Udp => _fixture.UdpAnnounceUrl,
            _ => throw new Exception($"Unknown type {announceType}"),
        };

        var logger = loggerFactory.CreateLogger("Torrent");

        var seeder = new TorrentClient(logger: logger);
        var leecher = new TorrentClient(logger: logger);

        using var cts = TestContext.Current.CancellationToken.WithTimeout(1.Minutes());

        await using var seederTorrent = await seeder.CreateTorrentAsync(
            "Data/test.txt",
            announceUrl,
            [announceUrl],
            cancellationToken: cts.Token
        );

        await using var leecherTorrent = leecher.ImportTorrent(seederTorrent.MetaInfo, "Output");

        seederTorrent.Start(cts.Token);
        leecherTorrent.Start(cts.Token);

        await leecherTorrent.DownloadInfo.DownloadTask.ShouldNotThrowAsync();
    }
}
