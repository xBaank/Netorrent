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
[Timeout(60_000)]
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

        var seeders = await GetSeedersAsync(seedersCount, path, cancellationToken)
            .ToListAsync(cancellationToken: cancellationToken);
        var seedersTorrents = seeders.Select(i => i.Item1).ToList();

        var leechers = await GetLeechersAsync(
                leechersCount,
                seedersTorrents[0].MetaInfo,
                cancellationToken
            )
            .ToListAsync(cancellationToken: cancellationToken);
        var leechersTorrents = leechers.Select(i => i.Item1).ToList();

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
            var originalFile = await ReadAllBytesAsync(path, cancellationToken);
            var downloadedFile = await ReadAllBytesAsync(
                $"{leecherTorrent.OutputDirectory}/{leecherTorrent.MetaInfo.Info.Name}",
                cancellationToken
            );
            originalFile.SequenceEqual(downloadedFile).ShouldBeTrue();
        }

        foreach (var (_, client) in seeders)
        {
            await client.DisposeAsync();
        }

        foreach (var (_, client) in leechers)
        {
            await client.DisposeAsync();
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

        var seeders = await GetSeedersAsync(seedersCount, path, cancellationToken)
            .ToListAsync(cancellationToken: cancellationToken);
        var seedersTorrents = seeders.Select(i => i.Item1).ToList();

        var leechers = await GetLeechersAsync(
                leechersCount,
                seedersTorrents[0].MetaInfo,
                cancellationToken
            )
            .ToListAsync(cancellationToken: cancellationToken);
        var leechersTorrents = leechers.Select(i => i.Item1).ToList();

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

        foreach (var (_, client) in seeders)
        {
            await client.DisposeAsync();
        }

        foreach (var (_, client) in leechers)
        {
            await client.DisposeAsync();
        }
    }

    private async IAsyncEnumerable<(Torrent, TorrentClient)> GetSeedersAsync(
        int number,
        string path,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        for (int i = 0; i < number; i++)
        {
            var seeder = new TorrentClient(o => o with { PeerIpProxy = FixDockerAdress });

            var seederTorrent = await seeder.CreateTorrentAsync(
                path,
                _fixture.AnnounceUrl,
                [.. _fixture.AnnounceUrls],
                cancellationToken: cancellationToken
            );
            cancellationToken.Register(seederTorrent.Stop);

            yield return (seederTorrent, seeder);
        }
    }

    private static async IAsyncEnumerable<(Torrent, TorrentClient)> GetLeechersAsync(
        int number,
        MetaInfo metaInfo,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        for (int i = 0; i < number; i++)
        {
            var leecher = new TorrentClient(o => o with { PeerIpProxy = FixDockerAdress });

            var pathName = Guid.NewGuid().ToString();
            var leecherTorrent = leecher.ImportTorrent(metaInfo, $"Output/Test_{pathName}");
            cancellationToken.Register(leecherTorrent.Stop);

            yield return (leecherTorrent, leecher);
        }
    }

    [After(Class)]
    public static Task DisposeAsync(CancellationToken _)
    {
        if (Directory.Exists("Output"))
            Directory.Delete("Output", true);
        if (Directory.Exists("Input"))
            Directory.Delete("Input", true);
        return Task.CompletedTask;
    }
}
