using Netorrent.Bencoding;
using Netorrent.TorrentFile;
using Shouldly;

namespace Netorrent.Tests.TorrentFile;

public class TorrentTests
{
    [Theory]
    [InlineData("Data/evadubbed_archive.torrent")]
    [InlineData("Data/nosferatu.torrent")]
    public async Task CanDecodeTorrentFile(
        string path,
        CancellationToken cancellationToken = default
    )
    {
        var torrent = await Torrent.Create(path, cancellationToken);
        torrent.ShouldNotBeNull();
    }
}
