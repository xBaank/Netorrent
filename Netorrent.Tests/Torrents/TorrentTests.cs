using System.Net;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.Tests.Fixtures;
using Netorrent.TorrentFile;
using Shouldly;
using TimeSpanXt;

namespace Netorrent.Tests.Torrents;

public enum AnnounceType
{
    Http,
    Udp,
}

public class TorrentTests(OpenTrackerFixture fixture, ITestOutputHelper outputHelper)
    : IClassFixture<OpenTrackerFixture>,
        IAsyncDisposable
{
    private readonly OpenTrackerFixture _fixture = fixture;

    [Theory]
    [InlineData(AnnounceType.Http)]
    [InlineData(AnnounceType.Udp)]
    public async Task Should_download_torrent(AnnounceType announceType)
    {
        var loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Trace)
        );
        var announceUrl = announceType switch
        {
            AnnounceType.Http => _fixture.AnnounceUrl,
            AnnounceType.Udp => _fixture.UdpAnnounceUrl,
            _ => throw new Exception($"Unknown type {announceType}"),
        };

        var logger = loggerFactory.CreateLogger("Torrent");

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
            cancellationToken: TestContext.Current.CancellationToken
        );
        await using var leecherTorrent = leecher.ImportTorrent(seederTorrent.MetaInfo, "Output");

        using var cts = TestContext.Current.CancellationToken.WithTimeout(1.Minutes());
        cts.Token.Register(seederTorrent.Stop);
        cts.Token.Register(leecherTorrent.Stop);

        seederTorrent.Start();
        await Task.Delay(5.Seconds(), TestContext.Current.CancellationToken);
        leecherTorrent.Start();

        await leecherTorrent.DownloadInfo.DownloadTask.ShouldNotThrowAsync();
        seederTorrent.Stop();
        leecherTorrent.Stop();

        var originalFile = await ReadAllBytesAsync(
            "Data/test.txt",
            TestContext.Current.CancellationToken
        );

        var downloadedFile = await ReadAllBytesAsync(
            "Output/test.txt",
            TestContext.Current.CancellationToken
        );

        originalFile.SequenceEqual(downloadedFile).ShouldBeTrue();
    }

    [Theory]
    [InlineData(AnnounceType.Http)]
    [InlineData(AnnounceType.Udp)]
    public async Task Should_cancel_torrent(AnnounceType announceType)
    {
        var loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Trace)
        );
        var announceUrl = announceType switch
        {
            AnnounceType.Http => _fixture.AnnounceUrl,
            AnnounceType.Udp => _fixture.UdpAnnounceUrl,
            _ => throw new Exception($"Unknown type {announceType}"),
        };

        var logger = loggerFactory.CreateLogger("Torrent");

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
            cancellationToken: TestContext.Current.CancellationToken
        );
        await using var leecherTorrent = leecher.ImportTorrent(seederTorrent.MetaInfo, "Output");

        seederTorrent.Start();
        leecherTorrent.Start();
        seederTorrent.Stop();
        leecherTorrent.Stop();

        await leecherTorrent.DownloadInfo.DownloadTask.ShouldThrowAsync<TaskCanceledException>();
    }

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

    public ValueTask DisposeAsync()
    {
        if (File.Exists("Output/test.txt"))
            File.Delete("Output/test.txt");
        return ValueTask.CompletedTask;
    }
}
