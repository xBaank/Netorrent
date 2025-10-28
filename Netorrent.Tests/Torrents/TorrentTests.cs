using Netorrent.Tests.Fixtures;
using Netorrent.TorrentFile;

namespace Netorrent.Tests.Torrents;

public class TorrentTests(OpenTrackerFixture fixture) : IClassFixture<OpenTrackerFixture>
{
    private readonly OpenTrackerFixture _fixture = fixture;

    [Fact]
    public async Task Should_Download_Torrent()
    {
        var torrentClient = new TorrentClient();
        var torrent1 = await torrentClient.CreateTorrentAsync(
            "Data/test.txt",
            _fixture.AnnounceUrl,
            [_fixture.AnnounceUrl],
            cancellationToken: new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token
        );

        var torrent2 = torrentClient.AddTorrent(torrent1.MetaInfo);

        var torrent1Task = torrent1
            .StartAsync(new CancellationTokenSource(TimeSpan.FromSeconds(50)).Token)
            .AsTask();
        // Act
        var torrent2Task = torrent2
            .StartAsync(new CancellationTokenSource(TimeSpan.FromSeconds(50)).Token)
            .AsTask();

        await Task.WhenAll(torrent1Task, torrent2Task);
    }
}
