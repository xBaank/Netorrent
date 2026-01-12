using Netorrent.Bencoding;
using Netorrent.TorrentFile;
using Shouldly;

namespace Netorrent.Tests.Torrents;

[Timeout(5_000)]
public class TorrentFileTests
{
    [Test]
    public async Task Should_Export_Torrent_File(CancellationToken cancellationToken)
    {
        await using var torrentClient = new TorrentClient();
        await using var torrent = await torrentClient.LoadTorrentAsync(
            "Data/nosferatu.torrent",
            "Output",
            cancellationToken: cancellationToken
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

        torrent.Statistics.Transfer.DownloadedBytes.ShouldBe(0);
        torrent.Statistics.Transfer.VerifiedBytes.ShouldBe(0);

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
    public async Task Should_Create_Torrent_File_From_Directory(CancellationToken cancellationToken)
    {
        await using var torrentClient = new TorrentClient();
        await using var torrent = await torrentClient.CreateTorrentAsync(
            "Data/MultifileTest",
            "http://test.com",
            ["http://test.com"],
            ["http://test.com"],
            cancellationToken: cancellationToken
        );

        torrent.Statistics.Transfer.DownloadedBytes.ShouldBe(0);
        torrent.Statistics.Transfer.VerifiedBytes.ShouldBe(0);
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
    public async Task Should_Create_Torrent_File_From_File(CancellationToken cancellationToken)
    {
        await using var torrentClient = new TorrentClient();
        await using var torrent = await torrentClient.CreateTorrentAsync(
            "Data/MultifileTest/test.txt",
            "http://test.com",
            ["http://test.com"],
            ["http://test.com"],
            cancellationToken: cancellationToken
        );

        torrent.Statistics.Transfer.DownloadedBytes.ShouldBe(0);
        torrent.Statistics.Transfer.VerifiedBytes.ShouldBe(0);
        torrent.MetaInfo.Info.Type.ShouldBe(TorrentFile.FileStructure.InfoType.Single);
        torrent.MetaInfo.Info.Files.ShouldBeNull();
        torrent.MetaInfo.Info.Name.ShouldBe("test.txt");
    }

    [Test]
    public async Task Should_Not_Create_Torrent_File_From_Directory(
        CancellationToken cancellationToken
    )
    {
        await using var torrentClient = new TorrentClient();
        await torrentClient
            .CreateTorrentAsync(
                "Data/ASdasd",
                "http://test.com",
                ["http://test.com"],
                ["http://test.com"],
                cancellationToken: cancellationToken
            )
            .AsTask()
            .ShouldThrowAsync<FileNotFoundException>();
    }

    [Test]
    public async Task Should_Not_Create_Torrent_File_From_File(CancellationToken cancellationToken)
    {
        await using var torrentClient = new TorrentClient();
        await torrentClient
            .CreateTorrentAsync(
                "Data/MultifileTest/adasdasd.txt",
                "http://test.com",
                ["http://test.com"],
                ["http://test.com"],
                cancellationToken: cancellationToken
            )
            .AsTask()
            .ShouldThrowAsync<FileNotFoundException>();
    }

    [Test]
    [Arguments("Data/MultifileTest/test.txt")]
    [Arguments("Data/MultifileTest")]
    public async Task Should_Verify_File(string path, CancellationToken cancellationToken)
    {
        var pieceLength = 256 * 1024;
        await using var torrentClient = new TorrentClient();
        await using var torrent = await torrentClient.CreateTorrentAsync(
            path,
            "http://test.com",
            ["http://test.com"],
            ["http://test.com"],
            pieceLength: pieceLength,
            cancellationToken: cancellationToken
        );

        await torrent.VerifyAsync(cancellationToken);

        torrent.Statistics.Transfer.DownloadedBytes.ShouldBe(pieceLength * torrent.Bitfield.Length);
        torrent.Statistics.Transfer.VerifiedBytes.ShouldBe(pieceLength * torrent.Bitfield.Length);
        torrent.Bitfield.DownloadedPieces.Count.ShouldBe(torrent.Bitfield.Length);
    }

    [Test]
    [Arguments("Data/CorruptfileTest/test.txt", "Data/CorruptfileTest/test.txt")]
    [Arguments("Data/CorruptfileTest/Folder1", "Data/CorruptfileTest/Folder1/Folder2/test3.txt")]
    public async Task Should_Verify_Corrupt_File(
        string path,
        string fileToModify,
        CancellationToken cancellationToken
    )
    {
        var pieceLength = 256 * 1024;
        var corruptedPieceCount = 2;
        var startIndex = corruptedPieceCount * pieceLength;
        var size = corruptedPieceCount * pieceLength;
        var emptyData = new byte[size];
        await using var torrentClient = new TorrentClient();
        await using var torrent = await torrentClient.CreateTorrentAsync(
            path,
            "http://test.com",
            ["http://test.com"],
            ["http://test.com"],
            pieceLength: pieceLength,
            cancellationToken: cancellationToken
        );
        var expectedPieceCount = torrent.Bitfield.Length - corruptedPieceCount;

        await using (
            var fileStream = new FileStream(
                fileToModify,
                FileMode.Open,
                FileAccess.Write,
                FileShare.ReadWrite
            )
        )
        {
            fileStream.Seek(startIndex, SeekOrigin.Begin);
            await fileStream.WriteAsync(emptyData, cancellationToken);
        }
        await torrent.VerifyAsync(cancellationToken);

        torrent.Statistics.Transfer.DownloadedBytes.ShouldBe(pieceLength * expectedPieceCount);
        torrent.Statistics.Transfer.VerifiedBytes.ShouldBe(pieceLength * expectedPieceCount);
        torrent.Bitfield.DownloadedPieces.Count.ShouldBe(expectedPieceCount);
    }
}
