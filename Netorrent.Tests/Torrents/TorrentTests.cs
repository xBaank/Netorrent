using Netorrent.Tests.Fixtures;
using Netorrent.TorrentFile;

namespace Netorrent.Tests.Torrents;

internal class TorrentTests(OpenTrackerFixture fixture) : IClassFixture<OpenTrackerFixture>
{
    private readonly OpenTrackerFixture _fixture = fixture;

    public async Task Should_Download_Torrent()
    {
        // Arrange
        var metaInfo = TestMetaInfoFactory.CreateSingleFileMetaInfo(
            _fixture.AnnounceUrl,
            "test.a",
            "ONE;TWO;THREEEE"
        );
        var peer1 = new Torrent(metaInfo);
        var peer2 = new Torrent(metaInfo);

        // Act
        var downloadTask = peer1.DownloadAll(
            new CancellationTokenSource(TimeSpan.FromSeconds(30)).Token
        );
    }
}
