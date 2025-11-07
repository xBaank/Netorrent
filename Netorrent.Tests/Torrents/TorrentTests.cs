using MartinCostello.Logging.XUnit;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.Other;
using Netorrent.Tests.Fixtures;
using Netorrent.TorrentFile;
using TimeSpanXt;
using Xunit.Sdk;

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

        logger.LogInformation("Logger is working inside test!");
        outputHelper.WriteLine("OutputHelper works directly!");

        var torrentSeeder = new TorrentClient(logger: logger);
        var torrentSeeder2 = new TorrentClient(logger: logger);
        var torrentLeecher = new TorrentClient(logger: logger);

        using var cts = TestContext.Current.CancellationToken.WithTimeout(30.Seconds());

        var torrent1 = await torrentSeeder.CreateTorrentAsync(
            "Data/test.txt",
            _fixture.AnnounceUrl,
            [_fixture.AnnounceUrl],
            cancellationToken: cts.Token
        );
        var torrent2 = await torrentSeeder.CreateTorrentAsync(
            "Data/test.txt",
            _fixture.AnnounceUrl,
            [_fixture.AnnounceUrl],
            cancellationToken: cts.Token
        );

        var torrent3 = torrentLeecher.AddTorrent(torrent1.MetaInfo, "Output");

        var torrent1Task = torrent1.StartAsync(cts.Token);
        await Task.Delay(5.Seconds(), cts.Token);
        var torrent2Task = torrent2.StartAsync(cts.Token);
        var torrent3Task = torrent3.StartAsync(cts.Token);

        await TaskUtils.WhenAllOrOneThrows(torrent1Task, torrent2Task, torrent3Task);
    }
}
