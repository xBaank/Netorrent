using Netorrent.TorrentFile;
using Shouldly;

namespace Netorrent.Tests.Metadata;

public class MetadataTests
{
    [Theory]
    [InlineData("Data/evadubbed_archive.torrent")]
    [InlineData("Data/nosferatu.torrent")]
    public async Task CanDecodeTorrentFile(
        string path,
        CancellationToken cancellationToken = default
    )
    {
        var torrent = await Torrent.AddTorrentAsync(path, cancellationToken);
        torrent.ShouldNotBeNull();
        torrent.MetaInfo.ShouldNotBeNull();
        torrent.MetaInfo.Announce.ShouldNotBeNullOrWhiteSpace();
        torrent.MetaInfo.Info.Name.ShouldNotBeNullOrWhiteSpace();
        torrent.MetaInfo.Info.PieceLength.ShouldBeGreaterThan(0);
        torrent.MetaInfo.Info.Pieces.ShouldNotBeNullOrWhiteSpace();
        torrent.MetaInfo.Info.InfoHash.ShouldNotBeNull();
    }
}
