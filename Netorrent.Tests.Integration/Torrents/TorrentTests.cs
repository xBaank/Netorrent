using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.Tests.Integration.Fixtures;
using Netorrent.TorrentFile;
using Netorrent.TorrentFile.FileStructure;
using Shouldly;

namespace Netorrent.Tests.Integration.Torrents;

[ClassDataSource<OpenTrackerFixture>(Shared = SharedType.PerClass)]
[Timeout(6 * 60_000)]
public class TorrentTests(OpenTrackerFixture fixture)
{
    private readonly OpenTrackerFixture _fixture = fixture;
    private static ILogger Logger => new TUnitLogger(TestContext.Current!.GetDefaultLogger());

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

        long size = 10L * 1024 * 1024; // 10 MB
        for (long i = 0; i < size; i++)
        {
            stream.WriteByte((byte)Random.Shared.Next());
        }
        return path;
    }

    [Test]
    [MatrixDataSource]
    public async Task Should_Download_Torrent_With_Different_Ips_And_Trackers(
        [Matrix(UsedTrackers.Http, UsedTrackers.Udp, UsedTrackers.Http | UsedTrackers.Udp)]
            UsedTrackers usedTrackers,
        [Matrix(
            UsedAddressProtocol.Ipv4,
            UsedAddressProtocol.Ipv6,
            UsedAddressProtocol.Ipv4 | UsedAddressProtocol.Ipv6
        )]
            UsedAddressProtocol usedAdressProtocol,
        [Matrix(4)] int seedersCount,
        [Matrix(30)] int leechersCount,
        CancellationToken cancellationToken
    )
    {
        if (!Socket.OSSupportsIPv6 && usedAdressProtocol.HasFlag(UsedAddressProtocol.Ipv6))
        {
            Skip.Test("Ipv6 is not supported");
        }

        if (!Socket.OSSupportsIPv4 && usedAdressProtocol.HasFlag(UsedAddressProtocol.Ipv4))
        {
            Skip.Test("Ipv4 is not supported");
        }

        var path = await CreateRandomFileAsync("Input");

        var seeders = await GetSeedersAsync(
                seedersCount,
                path,
                Logger,
                usedTrackers,
                usedAdressProtocol,
                0.Seconds
            )
            .ToListAsync(cancellationToken: cancellationToken);
        var seedersTorrents = seeders.Select(i => i.Item1).ToList();

        var leechers = await GetLeechersAsync(
                leechersCount,
                seedersTorrents[0].MetaInfo,
                Logger,
                usedTrackers,
                usedAdressProtocol,
                0.Seconds
            )
            .ToListAsync(cancellationToken: cancellationToken);
        var leechersTorrents = leechers.Select(i => i.Item1).ToList();

        try
        {
            foreach (var seederTorrent in seedersTorrents)
            {
                cancellationToken.Register(seederTorrent.Stop);
                await seederTorrent.StartAsync();
            }

            //This is needed because if seeder and leecher announce at the same time they don't see each other
            await Task.Delay(5000, cancellationToken);

            foreach (var leecherTorrent in leechersTorrents)
            {
                cancellationToken.Register(leecherTorrent.Stop);
                await leecherTorrent.StartAsync();
            }

            foreach (var leecherTorrent in leechersTorrents)
            {
                await leecherTorrent.Completion.AsTask().ShouldNotThrowAsync();
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
                leecherTorrent.Statistics.Transfer.VerifiedBytes.ShouldBe(downloadedFile.Length);
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
        [MatrixRange<int>(3, 6)] int seedersCount,
        [MatrixRange<int>(3, 6)] int leechersCount,
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
                cancellationToken.Register(seederTorrent.Stop);
                await seederTorrent.StartAsync();
            }

            await Task.Delay(1.Seconds, cancellationToken);

            foreach (var leecherTorrent in leechersTorrents)
            {
                cancellationToken.Register(leecherTorrent.Stop);
                await leecherTorrent.StartAsync();
            }

            foreach (var seederTorrent in seedersTorrents)
            {
                await seederTorrent.StopAsync();
            }

            foreach (var leecherTorrent in leechersTorrents)
            {
                await leecherTorrent.StopAsync();
            }

            foreach (var seederTorrent in seedersTorrents)
            {
                await seederTorrent.Completion.AsTask().ShouldNotThrowAsync();
            }

            foreach (var leecherTorrent in leechersTorrents)
            {
                await leecherTorrent.Completion.AsTask().ShouldThrowAsync<TaskCanceledException>();
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
    public async Task Should_Throw_In_Torrent(
        [MatrixRange<int>(3, 6)] int seedersCount,
        [MatrixRange<int>(3, 6)] int leechersCount,
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
                cancellationToken.Register(seederTorrent.Stop);
                await seederTorrent.StartAsync();
            }

            await Task.Delay(1.Seconds, cancellationToken);

            foreach (var leecherTorrent in leechersTorrents)
            {
                cancellationToken.Register(leecherTorrent.Stop);
                await leecherTorrent.StartAsync();
            }

            foreach (var leecherTorrent in leechersTorrents)
            {
                leecherTorrent.Completion.TrySetException(new InvalidOperationException());
            }

            foreach (var seederTorrent in seedersTorrents)
            {
                await seederTorrent.StopAsync();
            }

            foreach (var leecherTorrent in leechersTorrents)
            {
                await leecherTorrent.StopAsync();
            }

            foreach (var seederTorrent in seedersTorrents)
            {
                await seederTorrent.Completion.AsTask().ShouldNotThrowAsync();
            }

            foreach (var leecherTorrent in leechersTorrents)
            {
                await leecherTorrent
                    .Completion.AsTask()
                    .ShouldThrowAsync<InvalidOperationException>();
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

    private async IAsyncEnumerable<(Torrent, TorrentClient)> GetSeedersAsync(
        int number,
        string path,
        ILogger logger,
        UsedTrackers usedTrackers = UsedTrackers.Http | UsedTrackers.Udp,
        UsedAddressProtocol usedAdressProtocol =
            UsedAddressProtocol.Ipv4 | UsedAddressProtocol.Ipv6,
        TimeSpan? warmupTime = null
    )
    {
        for (int i = 0; i < number; i++)
        {
            var seeder = new TorrentClient(o =>
                GetOptions(logger, usedTrackers, usedAdressProtocol, warmupTime, o)
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
        ILogger logger,
        UsedTrackers usedTrackers = UsedTrackers.Http | UsedTrackers.Udp,
        UsedAddressProtocol usedAdressProtocol =
            UsedAddressProtocol.Ipv4 | UsedAddressProtocol.Ipv6,
        TimeSpan? warmupTime = null
    )
    {
        for (int i = 0; i < number; i++)
        {
            var leecher = new TorrentClient(o =>
                GetOptions(logger, usedTrackers, usedAdressProtocol, warmupTime, o)
            );

            var pathName = Guid.NewGuid().ToString();
            var leecherTorrent = leecher.LoadTorrent(metaInfo, $"Output/Test_{pathName}");

            yield return (leecherTorrent, leecher);
        }
    }

    private static TorrentClientOptions GetOptions(
        ILogger logger,
        UsedTrackers usedTrackers,
        UsedAddressProtocol usedAdressProtocol,
        TimeSpan? warmupTime,
        TorrentClientOptions o
    ) =>
        o with
        {
            PeerIpProxy = iPAddress =>
            {
                //Because docker use NAT and host mode doesn't work properly in win or mac we need to transform those ips.
                if (usedAdressProtocol.HasFlag(UsedAddressProtocol.Ipv4))
                {
                    return IPAddress.Loopback;
                }

                if (usedAdressProtocol.HasFlag(UsedAddressProtocol.Ipv6))
                {
                    return IPAddress.IPv6Loopback;
                }
                throw new Exception("No protocol specified");
            },
            WarmupTime = warmupTime ?? 8.Seconds,
            Logger = logger,
            UsedTrackers = usedTrackers,
            UsedAdressProtocol = usedAdressProtocol,
        };
}
