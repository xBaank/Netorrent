using System.Net;
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
[Timeout(3 * 60_000)]
public class TorrentTests(OpenTrackerFixture fixture)
{
    private readonly OpenTrackerFixture _fixture = fixture;
    private static ILogger Logger => new TUnitLogger(TestContext.Current!.GetDefaultLogger());

    private static IPAddress FixDockerAdress(IPAddress iPAddress) =>
        iPAddress.ToString().StartsWith("172.") ? IPAddress.Loopback : iPAddress;

    private static async Task<byte[]> ReadAllBytesAsync(
        string path,
        CancellationToken cancellationToken = default
    )
    {
        await using var stream = new FileStream(
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

    private static async ValueTask<string> CreateRandomFileAsync(string folder)
    {
        var guid = Guid.NewGuid().ToString();
        var path = Path.Combine(folder, $"Test_{guid}");
        if (!Directory.Exists(folder))
            Directory.CreateDirectory(folder);
        await using var stream = new FileStream(
            path,
            FileMode.Create,
            FileAccess.Write,
            FileShare.None,
            bufferSize: 81920,
            options: FileOptions.Asynchronous | FileOptions.SequentialScan
        );

        long size = 10L * 1024 * 1024; // 100 MB
        for (long i = 0; i < size; i++)
        {
            stream.WriteByte((byte)Random.Shared.Next());
        }
        return path;
    }

    [Test]
    [MatrixDataSource]
    public async Task Should_Download_Torrent(
        [MatrixRange<int>(1, 5)] int seedersCount,
        [MatrixRange<int>(1, 5)] int leechersCount,
        CancellationToken cancellationToken
    )
    {
        var path = await CreateRandomFileAsync("Input");

        var seeders = await GetSeedersAsync(seedersCount, path, Logger)
            .ToListAsync(cancellationToken: cancellationToken);
        var seedersTorrents = seeders.Select(i => i.Item1).ToList();

        var leechers = await GetLeechersAsync(leechersCount, seedersTorrents[0].MetaInfo, Logger)
            .ToListAsync(cancellationToken: cancellationToken);
        var leechersTorrents = leechers.Select(i => i.Item1).ToList();

        try
        {
            foreach (var seederTorrent in seedersTorrents)
            {
                await seederTorrent.StartAsync(cancellationToken);
            }

            foreach (var leecherTorrent in leechersTorrents)
            {
                await leecherTorrent.StartAsync(cancellationToken);
            }

            foreach (var leecherTorrent in leechersTorrents)
            {
                await leecherTorrent.DownloadInfo.DownloadTask.ShouldNotThrowAsync();
            }

            foreach (var seederTorrent in seedersTorrents)
            {
                await seederTorrent.StopAsync();
            }

            foreach (var leecherTorrent in leechersTorrents)
            {
                await leecherTorrent.StopAsync();
            }

            foreach (var leecherTorrent in leechersTorrents)
            {
                var originalFile = await ReadAllBytesAsync(path, cancellationToken);
                var downloadedFile = await ReadAllBytesAsync(
                    $"{leecherTorrent.OutputDirectory}/{leecherTorrent.MetaInfo.Info.Name}",
                    cancellationToken
                );
                originalFile.SequenceEqual(downloadedFile).ShouldBeTrue();
            }
        }
        finally
        {
            foreach (var (_, client) in seeders)
            {
                await client.DisposeAsync();
            }
            foreach (var (_, client) in leechers)
            {
                await client.DisposeAsync();
            }
        }
    }

    [Test]
    [MatrixDataSource]
    public async Task Should_Stop_Torrent(
        [MatrixRange<int>(1, 3)] int seedersCount,
        [MatrixRange<int>(1, 3)] int leechersCount,
        CancellationToken cancellationToken
    )
    {
        var path = await CreateRandomFileAsync("Input");

        var seeders = await GetSeedersAsync(seedersCount, path, Logger)
            .ToListAsync(cancellationToken: cancellationToken);
        var seedersTorrents = seeders.Select(i => i.Item1).ToList();

        var leechers = await GetLeechersAsync(leechersCount, seedersTorrents[0].MetaInfo, Logger)
            .ToListAsync(cancellationToken: cancellationToken);
        var leechersTorrents = leechers.Select(i => i.Item1).ToList();

        try
        {
            foreach (var seederTorrent in seedersTorrents)
            {
                await seederTorrent.StartAsync(cancellationToken);
            }

            foreach (var leecherTorrent in leechersTorrents)
            {
                await leecherTorrent.StartAsync(cancellationToken);
            }

            foreach (var seederTorrent in seedersTorrents)
            {
                await seederTorrent.StopAsync();
            }

            foreach (var leecherTorrent in leechersTorrents)
            {
                await leecherTorrent.StopAsync();
            }

            foreach (var leecherTorrent in leechersTorrents)
            {
                await leecherTorrent.DownloadInfo.DownloadTask.ShouldThrowAsync<TaskCanceledException>();
            }
        }
        finally
        {
            foreach (var (_, client) in seeders)
            {
                await client.DisposeAsync();
            }

            foreach (var (_, client) in leechers)
            {
                await client.DisposeAsync();
            }
        }
    }

    [Test]
    public async Task Should_Download_Real_Torrent(CancellationToken cancellationToken)
    {
        await using var torrentClient = new TorrentClient(o => o with { Logger = Logger });
        await using var torrent = await torrentClient.ImportTorrentAsync(
            "Data/debian-13.2.0-amd64-netinst.iso.torrent",
            "Output",
            cancellationToken
        );
        await torrent.StartAsync(cancellationToken);
        await torrent.DownloadInfo.DownloadTask.ShouldNotThrowAsync();
        await torrent.StopAsync();
    }

    private async IAsyncEnumerable<(Torrent, TorrentClient)> GetSeedersAsync(
        int number,
        string path,
        ILogger logger
    )
    {
        for (int i = 0; i < number; i++)
        {
            var seeder = new TorrentClient(o =>
                o with
                {
                    PeerIpProxy = FixDockerAdress,
                    //     Logger = logger,
                }
            );

            var seederTorrent = await seeder.CreateTorrentAsync(
                path,
                _fixture.AnnounceUrl,
                [.. _fixture.AnnounceUrls]
            );

            yield return (seederTorrent, seeder);
        }
    }

    private static async IAsyncEnumerable<(Torrent, TorrentClient)> GetLeechersAsync(
        int number,
        MetaInfo metaInfo,
        ILogger logger
    )
    {
        for (int i = 0; i < number; i++)
        {
            var leecher = new TorrentClient(o =>
                o with
                {
                    PeerIpProxy = FixDockerAdress,
                    //     Logger = logger,
                }
            );

            var pathName = Guid.NewGuid().ToString();
            var leecherTorrent = leecher.ImportTorrent(metaInfo, $"Output/Test_{pathName}");

            yield return (leecherTorrent, leecher);
        }
    }
}
