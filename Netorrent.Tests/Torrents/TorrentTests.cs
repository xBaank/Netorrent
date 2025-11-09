using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.Tests.Fixtures;
using Netorrent.TorrentFile;
using TimeSpanXt;

namespace Netorrent.Tests.Torrents;

public class TorrentTests(OpenTrackerFixture fixture, ITestOutputHelper outputHelper)
    : IClassFixture<OpenTrackerFixture>
{
    private readonly OpenTrackerFixture _fixture = fixture;

    [Fact]
    public async Task Should_download_torrent()
    {
        // Arrange
        var loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Trace)
        );

        var logger = loggerFactory.CreateLogger("Torrent");

        var seeder = new TorrentClient(logger: logger);
        var leecher = new TorrentClient(logger: logger);

        using var cts = TestContext.Current.CancellationToken.WithTimeout(30.Seconds());

        var seederTorrent = await seeder.CreateTorrentAsync(
            "Data/test.txt",
            _fixture.AnnounceUrl,
            [_fixture.AnnounceUrl],
            cancellationToken: cts.Token
        );

        var leecherTorrent = leecher.AddTorrent(seederTorrent.MetaInfo, "Output");

        seederTorrent.Start(cts.Token);
        leecherTorrent.Start(cts.Token);

        await leecherTorrent.DownloadInfo.DownloadTask;
    }
}
