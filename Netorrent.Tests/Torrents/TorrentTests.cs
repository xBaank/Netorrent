using Netorrent.Other;
using Netorrent.Tests.Fixtures;
using Netorrent.TorrentFile;
using TimeSpanXt;

namespace Netorrent.Tests.Torrents;

public class TorrentTests(OpenTrackerFixture fixture) : IClassFixture<OpenTrackerFixture>
{
    private readonly OpenTrackerFixture _fixture = fixture;

    [Fact]
    public async Task Should_Download_Torrent()
    {
        var torrentSeeder = new TorrentClient();
        var torrentLeecher = new TorrentClient();

        var torrent1 = await torrentSeeder.CreateTorrentAsync(
            "Data/test.txt",
            _fixture.AnnounceUrl,
            [_fixture.AnnounceUrl],
            cancellationToken: TestContext.Current.CancellationToken
        );

        var torrent2 = torrentLeecher.AddTorrent(torrent1.MetaInfo, "Output");

        var torrent1Task = torrent1.StartAsync(TestContext.Current.CancellationToken);
        await Task.Delay(5.Seconds(), TestContext.Current.CancellationToken);
        // Act
        var torrent2Task = torrent2.StartAsync(TestContext.Current.CancellationToken);

        await TaskUtils.WhenAllOrOneThrows(torrent1Task, torrent2Task);
    }
}
