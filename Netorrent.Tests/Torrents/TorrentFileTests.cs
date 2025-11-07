using Netorrent.Bencoding;
using Netorrent.TorrentFile;
using Shouldly;

namespace Netorrent.Tests.Torrents;

public class TorrentFileTests
{
    [Fact]
    public async Task Should_export_torrent_file()
    {
        var torrentClient = new TorrentClient();
        var torrent = await torrentClient.AddTorrentAsync(
            "Data/nosferatu.torrent",
            "Output",
            TestContext.Current.CancellationToken
        );
        File.Delete("Output/asd.torrent");
        await torrent.ExportAsync("Output/asd.torrent", TestContext.Current.CancellationToken);

        var decoder = new BDecoder(
            await File.ReadAllBytesAsync(
                "Data/nosferatu.torrent",
                TestContext.Current.CancellationToken
            )
        );

        var decoder2 = new BDecoder(
            await File.ReadAllBytesAsync(
                "Output/asd.torrent",
                TestContext.Current.CancellationToken
            )
        );

        var original = decoder.DecodeDic();
        var expected = decoder2.DecodeDic();

        var originalMetainfo = TorrentClient.ParseMetaInfo(original);
        var expectedMetainfo = TorrentClient.ParseMetaInfo(expected);

        originalMetainfo.Announce.ShouldBeEquivalentTo(expectedMetainfo.Announce);
        originalMetainfo.AnnounceList.ShouldBeEquivalentTo(expectedMetainfo.AnnounceList);
        originalMetainfo.Comment.ShouldBeEquivalentTo(expectedMetainfo.Comment);
        originalMetainfo.CreatedBy.ShouldBeEquivalentTo(expectedMetainfo.CreatedBy);
        originalMetainfo.CreationDate.ShouldBeEquivalentTo(expectedMetainfo.CreationDate);
        originalMetainfo.Encoding.ShouldBeEquivalentTo(expectedMetainfo.Encoding);
        originalMetainfo.Title.ShouldBeEquivalentTo(expectedMetainfo.Title);
        originalMetainfo.UrlList.ShouldBeEquivalentTo(expectedMetainfo.UrlList);

        originalMetainfo.Info.Files.ShouldBeEquivalentTo(expectedMetainfo.Info.Files);
        originalMetainfo.Info.Length.ShouldBeEquivalentTo(expectedMetainfo.Info.Length);
        originalMetainfo.Info.Md5sum.ShouldBeEquivalentTo(expectedMetainfo.Info.Md5sum);
        originalMetainfo.Info.PieceLength.ShouldBeEquivalentTo(expectedMetainfo.Info.PieceLength);
        originalMetainfo.Info.Pieces.ShouldBeEquivalentTo(expectedMetainfo.Info.Pieces);
        originalMetainfo.Info.Private.ShouldBeEquivalentTo(expectedMetainfo.Info.Private);
        originalMetainfo.Info.Name.ShouldBeEquivalentTo(expectedMetainfo.Info.Name);
        originalMetainfo.Info.Type.ShouldBeEquivalentTo(expectedMetainfo.Info.Type);

        originalMetainfo.Info.InfoHash.ShouldBeEquivalentTo(expectedMetainfo.Info.InfoHash);
    }
}
