using System.Net;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.Tests.Fixtures;
using Netorrent.TorrentFile;
using Shouldly;

namespace Netorrent.Tests.Torrents;

public enum AnnounceType
{
    Http,
    Udp,
}

[ClassDataSource<OpenTrackerFixture>(Shared = SharedType.PerClass)]
public class TorrentTests(OpenTrackerFixture fixture)
{
    private readonly OpenTrackerFixture _fixture = fixture;

    private static IPAddress FixDockerAdress(IPAddress iPAddress) =>
        iPAddress.ToString().StartsWith("172.") ? IPAddress.Loopback : iPAddress;

    private static async Task<byte[]> ReadAllBytesAsync(
        string path,
        CancellationToken cancellationToken = default
    )
    {
        using var stream = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite,
            bufferSize: 4096,
            options: FileOptions.Asynchronous | FileOptions.SequentialScan
        );

        var buffer = new byte[stream.Length];
        int totalRead = 0;

        while (totalRead < buffer.Length)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(totalRead), cancellationToken);

            if (read == 0)
                break;

            totalRead += read;
        }

        return buffer;
    }

    [Test]
    [Timeout(60_000)]
    [Arguments(AnnounceType.Http)]
    [Arguments(AnnounceType.Udp)]
    public async Task Should_download_torrent(
        AnnounceType announceType,
        CancellationToken cancellationToken
    )
    {
        var announceUrl = announceType switch
        {
            AnnounceType.Http => _fixture.AnnounceUrl,
            AnnounceType.Udp => _fixture.UdpAnnounceUrl,
            _ => throw new Exception($"Unknown type {announceType}"),
        };

        var logger = new TUnitLogger(TestContext.Current!.GetDefaultLogger());

        var seeder = new TorrentClient(o =>
            o with
            {
                Logger = logger,
                PeerIpProxy = FixDockerAdress,
            }
        );
        var leecher = new TorrentClient(o =>
            o with
            {
                Logger = logger,
                PeerIpProxy = FixDockerAdress,
            }
        );

        await using var seederTorrent = await seeder.CreateTorrentAsync(
            "Data/test.txt",
            announceUrl,
            [announceUrl],
            cancellationToken: cancellationToken
        );
        await using var leecherTorrent = leecher.ImportTorrent(seederTorrent.MetaInfo, "Output");

        cancellationToken.Register(seederTorrent.Stop);
        cancellationToken.Register(leecherTorrent.Stop);

        seederTorrent.Start();
        leecherTorrent.Start();

        await leecherTorrent.DownloadInfo.DownloadTask.ShouldNotThrowAsync();
        seederTorrent.Stop();
        leecherTorrent.Stop();

        var originalFile = await ReadAllBytesAsync("Data/test.txt", cancellationToken);

        var downloadedFile = await ReadAllBytesAsync("Output/test.txt", cancellationToken);

        originalFile.SequenceEqual(downloadedFile).ShouldBeTrue();
    }

    [Test]
    [Timeout(60_000)]
    [Arguments(AnnounceType.Http)]
    [Arguments(AnnounceType.Udp)]
    public async Task Should_cancel_torrent(
        AnnounceType announceType,
        CancellationToken cancellationToken
    )
    {
        var announceUrl = announceType switch
        {
            AnnounceType.Http => _fixture.AnnounceUrl,
            AnnounceType.Udp => _fixture.UdpAnnounceUrl,
            _ => throw new Exception($"Unknown type {announceType}"),
        };

        var logger = new TUnitLogger(TestContext.Current!.GetDefaultLogger());

        var seeder = new TorrentClient(o =>
            o with
            {
                Logger = logger,
                PeerIpProxy = FixDockerAdress,
            }
        );
        var leecher = new TorrentClient(o =>
            o with
            {
                Logger = logger,
                PeerIpProxy = FixDockerAdress,
            }
        );

        await using var seederTorrent = await seeder.CreateTorrentAsync(
            "Data/test.txt",
            announceUrl,
            [announceUrl],
            cancellationToken: cancellationToken
        );
        await using var leecherTorrent = leecher.ImportTorrent(seederTorrent.MetaInfo, "Output");

        seederTorrent.Start();
        leecherTorrent.Start();
        seederTorrent.Stop();
        leecherTorrent.Stop();

        await leecherTorrent.DownloadInfo.DownloadTask.ShouldThrowAsync<TaskCanceledException>();
    }

    [After(Class)]
    public static Task DisposeAsync()
    {
        if (File.Exists("Output/test.txt"))
            File.Delete("Output/test.txt");
        return Task.CompletedTask;
    }
}
