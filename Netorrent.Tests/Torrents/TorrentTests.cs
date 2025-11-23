using System.Net;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.Tests.Fixtures;
using Netorrent.TorrentFile;
using Netorrent.TorrentFile.FileStructure;
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
    [MatrixDataSource]
    public async Task Should_download_torrent(
        [MatrixRange<int>(1, 3)] int seedersCount,
        [MatrixRange<int>(1, 3)] int leechersCount,
        CancellationToken cancellationToken
    )
    {
        var logger = new TUnitLogger(TestContext.Current!.GetDefaultLogger());

        var seedersTorrents = await GetSeedersAsync(seedersCount, logger, cancellationToken)
            .ToListAsync(cancellationToken: cancellationToken);

        var leechersTorrents = await GetLeechersAsync(
                leechersCount,
                seedersTorrents[0].MetaInfo,
                logger,
                cancellationToken
            )
            .ToListAsync(cancellationToken: cancellationToken);

        foreach (var seederTorrent in seedersTorrents)
        {
            seederTorrent.Start();
        }

        foreach (var leecherTorrent in leechersTorrents)
        {
            leecherTorrent.Start();
        }

        foreach (var leecherTorrent in leechersTorrents)
        {
            await leecherTorrent.DownloadInfo.DownloadTask.ShouldNotThrowAsync();
        }

        foreach (var seederTorrent in seedersTorrents)
        {
            seederTorrent.Stop();
        }

        foreach (var leecherTorrent in leechersTorrents)
        {
            leecherTorrent.Stop();
        }

        foreach (var leecherTorrent in leechersTorrents)
        {
            var originalFile = await ReadAllBytesAsync("Data/test.txt", cancellationToken);
            var downloadedFile = await ReadAllBytesAsync(
                $"{leecherTorrent.OutputDirectory}/test.txt",
                cancellationToken
            );
            originalFile.SequenceEqual(downloadedFile).ShouldBeTrue();
        }

        foreach (var seederTorrent in seedersTorrents)
        {
            await seederTorrent.DisposeAsync();
        }

        foreach (var leecherTorrent in leechersTorrents)
        {
            await leecherTorrent.DisposeAsync();
        }
    }

    [Test]
    [Timeout(60_000)]
    [MatrixDataSource]
    public async Task Should_cancel_torrent(
        [MatrixRange<int>(1, 3)] int seedersCount,
        [MatrixRange<int>(1, 3)] int leechersCount,
        CancellationToken cancellationToken
    )
    {
        var logger = new TUnitLogger(TestContext.Current!.GetDefaultLogger());

        var seedersTorrents = await GetSeedersAsync(seedersCount, logger, cancellationToken)
            .ToListAsync(cancellationToken: cancellationToken);

        var leechersTorrents = await GetLeechersAsync(
                leechersCount,
                seedersTorrents[0].MetaInfo,
                logger,
                cancellationToken
            )
            .ToListAsync(cancellationToken: cancellationToken);

        foreach (var seederTorrent in seedersTorrents)
        {
            seederTorrent.Start();
        }

        foreach (var leecherTorrent in leechersTorrents)
        {
            leecherTorrent.Start();
        }

        foreach (var seederTorrent in seedersTorrents)
        {
            seederTorrent.Stop();
        }

        foreach (var leecherTorrent in leechersTorrents)
        {
            leecherTorrent.Stop();
        }

        foreach (var leecherTorrent in leechersTorrents)
        {
            await leecherTorrent.DownloadInfo.DownloadTask.ShouldThrowAsync<TaskCanceledException>();
        }
    }

    private async IAsyncEnumerable<Torrent> GetSeedersAsync(
        int number,
        ILogger logger,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        for (int i = 0; i < number; i++)
        {
            var seeder = new TorrentClient(o =>
                o with
                {
                    Logger = logger,
                    PeerIpProxy = FixDockerAdress,
                }
            );

            var seederTorrent = await seeder.CreateTorrentAsync(
                "Data/test.txt",
                _fixture.AnnounceUrl,
                [.. _fixture.AnnounceUrls],
                cancellationToken: cancellationToken
            );

            yield return seederTorrent;
        }
    }

    private async IAsyncEnumerable<Torrent> GetLeechersAsync(
        int number,
        MetaInfo metaInfo,
        ILogger logger,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        for (int i = 0; i < number; i++)
        {
            var leecher = new TorrentClient(o =>
                o with
                {
                    Logger = logger,
                    PeerIpProxy = FixDockerAdress,
                }
            );

            var leecherTorrent = leecher.ImportTorrent(metaInfo, $"Output/Test_{i}");

            yield return leecherTorrent;
        }
    }

    [After(Class)]
    public static Task DisposeAsync()
    {
        if (Directory.Exists("Output"))
            Directory.Delete("Output", true);
        return Task.CompletedTask;
    }
}
