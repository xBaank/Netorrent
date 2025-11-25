using Netorrent.Bencoding;
using Netorrent.TorrentFile;
using Shouldly;

namespace Netorrent.Tests.Torrents;

public class TorrentFileTests
{
    [Test]
    public async Task Should_export_torrent_file(CancellationToken cancellationToken)
    {
        var torrentClient = new TorrentClient();
        var torrent = await torrentClient.ImportTorrentAsync(
            "Data/nosferatu.torrent",
            "Output",
            cancellationToken
        );
        File.Delete("Output/asd.torrent");
        await torrent.ExportAsync("Output/asd.torrent", cancellationToken);

        var decoder = new BDecoder(
            await File.ReadAllBytesAsync("Data/nosferatu.torrent", cancellationToken)
        );

        var decoder2 = new BDecoder(
            await File.ReadAllBytesAsync("Output/asd.torrent", cancellationToken)
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

    [Test]
    public async Task Should_create_torrent_file_from_directory(CancellationToken cancellationToken)
    {
        var torrentClient = new TorrentClient();
        var torrent = await torrentClient.CreateTorrentAsync(
            "Data/MultifileTest",
            "http://test.com",
            ["http://test.com"],
            ["http://test.com"],
            cancellationToken: cancellationToken
        );

        torrent.MetaInfo.Info.Type.ShouldBe(TorrentFile.FileStructure.InfoType.Multiple);
        torrent.MetaInfo.Info.Files.ShouldNotBeNull();
        torrent.MetaInfo.Info.Files.Count.ShouldBe(3);
        var filePath1 = string.Join("/", torrent.MetaInfo.Info.Files[0].Path);
        var filePath2 = string.Join("/", torrent.MetaInfo.Info.Files[1].Path);
        var filePath3 = string.Join("/", torrent.MetaInfo.Info.Files[2].Path);
        string[] paths = [filePath1, filePath2, filePath3];
        paths.ShouldContain("test.txt");
        paths.ShouldContain("Folder1/test2.txt");
        paths.ShouldContain("Folder1/Folder2/test3.txt");
    }

    [Test]
    public async Task Should_create_torrent_file_from_file(CancellationToken cancellationToken)
    {
        var torrentClient = new TorrentClient();
        var torrent = await torrentClient.CreateTorrentAsync(
            "Data/MultifileTest/test.txt",
            "http://test.com",
            ["http://test.com"],
            ["http://test.com"],
            cancellationToken: cancellationToken
        );

        torrent.MetaInfo.Info.Type.ShouldBe(TorrentFile.FileStructure.InfoType.Single);
        torrent.MetaInfo.Info.Files.ShouldBeNull();
        torrent.MetaInfo.Info.Name.ShouldBe("test.txt");
    }
}
