using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.Tests.Integration.Fixtures;
using Netorrent.TorrentFile;
using Netorrent.TorrentFile.FileStructure;
using Netorrent.TorrentFile.Options;
using Shouldly;

namespace Netorrent.Tests.Integration.Torrents;

[ClassDataSource<OpenTrackerFixture>(Shared = SharedType.PerClass)]
[Timeout(5 * 60_000)]
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
            {
                break;
            }

            totalRead += read;
        }

        return buffer;
    }

    private static async ValueTask<string> CreateRandomFileAsync(string folder)
    {
        var guid = Guid.NewGuid().ToString();
        var path = Path.Combine(folder, $"Test_{guid}");
        if (!Directory.Exists(folder))
        {
            Directory.CreateDirectory(folder);
        }

        await using var stream = new FileStream(
            path,
            FileMode.Create,
            FileAccess.Write,
            FileShare.None,
            bufferSize: 81920,
            options: FileOptions.SequentialScan
        );

        long size = 10L * 1024 * 1024; // 10 MB
        for (long i = 0; i < size; i++)
        {
            stream.WriteByte((byte)Random.Shared.Next(0, 255));
        }
        return path;
    }

    [Test]
    [MatrixDataSource]
    public async Task Should_Stop_Torrent(
        [Matrix(3)] int seedersCount,
        [Matrix(12)] int leechersCount,
        CancellationToken cancellationToken
    )
    {
        var path = await CreateRandomFileAsync("Input");

        var seeders = await GetSeedersAsync(seedersCount, path, Logger, warmupTime: 0.Seconds)
            .ToListAsync(cancellationToken: cancellationToken);
        var seedersTorrents = seeders.Select(i => i.Torrent).ToList();

        var leechers = await GetLeechersAsync(
                leechersCount,
                seedersTorrents[0].MetaInfo,
                Logger,
                warmupTime: 0.Seconds
            )
            .ToListAsync(cancellationToken: cancellationToken);
        var leechersTorrents = leechers.Select(i => i.Torrent).ToList();

        try
        {
            var seederStartTasks = seedersTorrents.Select(seederTorrent =>
            {
                cancellationToken.Register(seederTorrent.Stop);
                return seederTorrent.StartAsync().AsTask();
            });
            await Task.WhenAll(seederStartTasks);

            await Task.Delay(5000, cancellationToken);

            var leecherStartTasks = leechersTorrents.Select(leecherTorrent =>
            {
                cancellationToken.Register(leecherTorrent.Stop);
                return leecherTorrent.StartAsync().AsTask();
            });
            await Task.WhenAll(leecherStartTasks);

            var seederStopTasks = seedersTorrents.Select(seederTorrent =>
                seederTorrent.StopAsync().AsTask()
            );
            var leecherStopTasks = leechersTorrents.Select(leecherTorrent =>
                leecherTorrent.StopAsync().AsTask()
            );

            await Task.WhenAll([.. seederStopTasks, .. leecherStopTasks]);

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
    public async Task Should_Start_Stop_Start_Torrent(
        [Matrix(3)] int seedersCount,
        [Matrix(12)] int leechersCount,
        CancellationToken cancellationToken
    )
    {
        var path = await CreateRandomFileAsync("Input");

        var seeders = await GetSeedersAsync(seedersCount, path, Logger, warmupTime: 0.Seconds)
            .ToListAsync(cancellationToken: cancellationToken);
        var seedersTorrents = seeders.Select(i => i.Torrent).ToList();

        var leechers = await GetLeechersAsync(
                leechersCount,
                seedersTorrents[0].MetaInfo,
                Logger,
                warmupTime: 0.Seconds
            )
            .ToListAsync(cancellationToken: cancellationToken);
        var leechersTorrents = leechers.Select(i => i.Torrent).ToList();

        try
        {
            var seederStartTasks = seedersTorrents.Select(seederTorrent =>
            {
                cancellationToken.Register(seederTorrent.Stop);
                return seederTorrent.StartAsync().AsTask();
            });
            await Task.WhenAll(seederStartTasks);

            await Task.Delay(5000, cancellationToken);

            var leecherStartTasks = leechersTorrents.Select(leecherTorrent =>
            {
                cancellationToken.Register(leecherTorrent.Stop);
                return leecherTorrent.StartAsync().AsTask();
            });
            await Task.WhenAll(leecherStartTasks);

            await Task.Delay(5000, cancellationToken);

            var seederStopAsyncTasks = seedersTorrents.Select(i => i.StopAsync().AsTask());
            var leechersStopAsyncTasks = leechersTorrents.Select(i => i.StopAsync().AsTask());

            await Task.WhenAll([.. seederStopAsyncTasks, .. leechersStopAsyncTasks]);

            var seederRestartTasks = seedersTorrents.Select(seederTorrent =>
                seederTorrent.StartAsync().AsTask()
            );
            await Task.WhenAll(seederRestartTasks);

            await Task.Delay(5000, cancellationToken);

            var leecherRestartTasks = leechersTorrents.Select(leecherTorrent =>
                leecherTorrent.StartAsync().AsTask()
            );
            await Task.WhenAll(leecherRestartTasks);

            foreach (var leecherTorrent in leechersTorrents)
            {
                await leecherTorrent.Completion.AsTask().ShouldNotThrowAsync();
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
        [Matrix(3)] int seedersCount,
        [Matrix(12)] int leechersCount,
        CancellationToken cancellationToken
    )
    {
        var path = await CreateRandomFileAsync("Input");

        var seeders = await GetSeedersAsync(seedersCount, path, Logger, warmupTime: 0.Seconds)
            .ToListAsync(cancellationToken: cancellationToken);
        var seedersTorrents = seeders.Select(i => i.Torrent).ToList();

        var leechers = await GetLeechersAsync(
                leechersCount,
                seedersTorrents[0].MetaInfo,
                Logger,
                warmupTime: 0.Seconds
            )
            .ToListAsync(cancellationToken: cancellationToken);
        var leechersTorrents = leechers.Select(i => i.Torrent).ToList();

        try
        {
            var seederStartTasks = seedersTorrents.Select(seederTorrent =>
            {
                cancellationToken.Register(seederTorrent.Stop);
                return seederTorrent.StartAsync().AsTask();
            });
            await Task.WhenAll(seederStartTasks);

            await Task.Delay(5000, cancellationToken);

            var leecherStartTasks = leechersTorrents.Select(leecherTorrent =>
            {
                cancellationToken.Register(leecherTorrent.Stop);
                return leecherTorrent.StartAsync().AsTask();
            });
            await Task.WhenAll(leecherStartTasks);

            foreach (var leecherTorrent in leechersTorrents)
            {
                leecherTorrent.Completion.TrySetException(new InvalidOperationException());
            }

            var seederStopTasks = seedersTorrents.Select(seederTorrent =>
                seederTorrent.StopAsync().AsTask()
            );
            var leecherStopTasks = leechersTorrents.Select(leecherTorrent =>
                leecherTorrent.StopAsync().AsTask()
            );

            await Task.WhenAll([.. seederStopTasks, .. leecherStopTasks]);

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

    [Test]
    [MatrixDataSource]
    public async Task Should_Download_Torrent_With_Different_Ips_And_Trackers(
        [Matrix(UsedTrackers.Http, UsedTrackers.Udp, UsedTrackers.Http | UsedTrackers.Udp)]
            UsedTrackers usedTrackers,
        [MatrixMethod<TorrentTests>(nameof(GetAddressFamilies))] AddressFamily[] addressFamily,
        [Matrix(3)] int seedersCount,
        [Matrix(12)] int leechersCount,
        CancellationToken cancellationToken
    ) =>
        await TestDownloadAsync(
            usedTrackers,
            addressFamily,
            seedersCount,
            leechersCount,
            cancellationToken
        );

    [Test]
    public async Task Should_Download_Torrent(CancellationToken cancellationToken) =>
        await TestDownloadAsync(
            UsedTrackers.Http | UsedTrackers.Udp,
            null,
            4,
            50,
            cancellationToken
        );

    private async Task TestDownloadAsync(
        UsedTrackers usedTrackers,
        AddressFamily[]? addressFamilies,
        int seedersCount,
        int leechersCount,
        CancellationToken cancellationToken
    )
    {
        if (!Socket.OSSupportsIPv6 && addressFamilies.Contains(AddressFamily.InterNetworkV6))
        {
            Skip.Test("Ipv6 is not supported");
        }

        if (!Socket.OSSupportsIPv4 && addressFamilies.Contains(AddressFamily.InterNetwork))
        {
            Skip.Test("Ipv4 is not supported");
        }

        var path = await CreateRandomFileAsync("Input");

        var seeders = await GetSeedersAsync(
                seedersCount,
                path,
                Logger,
                addressFamilies,
                usedTrackers,
                0.Seconds
            )
            .ToListAsync(cancellationToken: cancellationToken);
        var seedersTorrents = seeders.Select(i => i.Torrent).ToList();

        var leechers = await GetLeechersAsync(
                leechersCount,
                seedersTorrents[0].MetaInfo,
                Logger,
                addressFamilies,
                usedTrackers,
                0.Seconds
            )
            .ToListAsync(cancellationToken: cancellationToken);
        var leechersTorrents = leechers.Select(i => i.Torrent).ToList();

        try
        {
            var seederStartTasks = seedersTorrents.Select(seederTorrent =>
            {
                cancellationToken.Register(seederTorrent.Stop);
                return seederTorrent.StartAsync().AsTask();
            });
            await Task.WhenAll(seederStartTasks);

            //This is needed because if seeder and leecher announce at the same time they don't see each other
            await Task.Delay(5000, cancellationToken);

            var leecherStartTasks = leechersTorrents.Select(leecherTorrent =>
            {
                cancellationToken.Register(leecherTorrent.Stop);
                return leecherTorrent.StartAsync().AsTask();
            });
            await Task.WhenAll(leecherStartTasks);

            foreach (var leecherTorrent in leechersTorrents)
            {
                await leecherTorrent.Completion.AsTask().ShouldNotThrowAsync();
            }

            var seederStopTasks = seedersTorrents.Select(seederTorrent =>
                seederTorrent.StopAsync().AsTask()
            );
            var leecherStopTasks = leechersTorrents.Select(leecherTorrent =>
                leecherTorrent.StopAsync().AsTask()
            );

            await Task.WhenAll([.. seederStopTasks, .. leecherStopTasks]);

            foreach (var leecherTorrent in leechersTorrents)
            {
                var originalFile = await ReadAllBytesAsync(path, cancellationToken);
                var downloadedFile = await ReadAllBytesAsync(
                    $"{leecherTorrent.OutputDirectory}/{leecherTorrent.MetaInfo.Info.Name}",
                    cancellationToken
                );
                originalFile.SequenceEqual(downloadedFile).ShouldBeTrue();
                leecherTorrent.Statistics.Data.Verified.ShouldBe(downloadedFile.Length);
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

    private async IAsyncEnumerable<(Torrent Torrent, TorrentClient Client)> GetSeedersAsync(
        int number,
        string path,
        ILogger logger,
        AddressFamily[]? addressFamilies = null,
        UsedTrackers usedTrackers = UsedTrackers.Http | UsedTrackers.Udp,
        TimeSpan? warmupTime = null
    )
    {
        for (int i = 0; i < number; i++)
        {
            var seeder = new TorrentClient(o =>
                GetOptions(logger, usedTrackers, addressFamilies, warmupTime, o)
            );

            var seederTorrent = await seeder.CreateTorrentAsync(
                path,
                _fixture.AnnounceUrl,
                [_fixture.AnnounceUrls]
            );

            yield return (seederTorrent, seeder);
        }
    }

    private static async IAsyncEnumerable<(Torrent Torrent, TorrentClient Client)> GetLeechersAsync(
        int number,
        MetaInfo metaInfo,
        ILogger logger,
        AddressFamily[]? addressFamilies = null,
        UsedTrackers usedTrackers = UsedTrackers.Http | UsedTrackers.Udp,
        TimeSpan? warmupTime = null
    )
    {
        for (int i = 0; i < number; i++)
        {
            var leecher = new TorrentClient(o =>
                GetOptions(logger, usedTrackers, addressFamilies, warmupTime, o)
            );

            var pathName = Guid.NewGuid().ToString();
            var leecherTorrent = leecher.LoadTorrent(metaInfo, $"Output/Test_{pathName}");

            yield return (leecherTorrent, leecher);
        }
    }

    private static IEnumerable<AddressFamily[]> GetAddressFamilies()
    {
        yield return [AddressFamily.InterNetwork];
        yield return [AddressFamily.InterNetworkV6];
        yield return [AddressFamily.InterNetwork, AddressFamily.InterNetworkV6];
    }

    private static TorrentClientOptions GetOptions(
        ILogger logger,
        UsedTrackers usedTrackers,
        AddressFamily[]? addressFamilies,
        TimeSpan? warmupTime,
        TorrentClientOptions o
    )
    {
        o = o with
        {
            PeerIpProxy = iPAddress =>
            {
                //Because docker use NAT and host mode doesn't work properly on win and mac we need to transform those ips.
                if (addressFamilies?.Contains(AddressFamily.InterNetwork) == true)
                {
                    return IPAddress.Loopback;
                }

                if (addressFamilies?.Contains(AddressFamily.InterNetworkV6) == true)
                {
                    return IPAddress.IPv6Loopback;
                }

                return iPAddress.AddressFamily switch
                {
                    AddressFamily.InterNetwork => IPAddress.Loopback,
                    AddressFamily.InterNetworkV6 => IPAddress.IPv6Loopback,
                    _ => throw new Exception($"Unsupported Address {iPAddress}"),
                };
            },
            WarmupTime = warmupTime ?? 8.Seconds,
            Logger = logger,
            UsedTrackers = usedTrackers,
        };

        if (addressFamilies is not null)
        {
            o = o with
            {
                ListenIpv4Address = addressFamilies
                    ?.Where(i => i == AddressFamily.InterNetwork)
                    .Select(i => IPAddress.Loopback)
                    .FirstOrDefault(),
                ListenIpv6Address = addressFamilies
                    ?.Where(i => i == AddressFamily.InterNetworkV6)
                    .Select(i => IPAddress.IPv6Loopback)
                    .FirstOrDefault(),
            };
        }

        return o;
    }
}
