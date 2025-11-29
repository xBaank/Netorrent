using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.P2P;
using Netorrent.TorrentFile.FileStructure;
using Netorrent.Tracker.Http;
using Netorrent.Tracker.Udp;

namespace Netorrent.Tracker;

internal class TrackerClient(
    HttpClient httpClient,
    UdpTrackerTransactionManager trackerTransaction,
    P2PClient p2PClient,
    PeerId peerId,
    ChannelWriter<IPEndPoint> trackersChannel,
    MetaInfo metaInfo,
    ILogger logger,
    IPAddress? forcedIp
) : IAsyncDisposable
{
    private readonly List<ITracker> _trackers = [];

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        List<string> announceList = [metaInfo.Announce, .. metaInfo.AnnounceList ?? []];
        var urls =
            announceList
                .Where(url => !string.IsNullOrWhiteSpace(url))
                .Distinct(StringComparer.OrdinalIgnoreCase)
            ?? [];

        var tasks = await CreateTrackers(urls, cancellationToken)
            .Select(i => i.StartAsync(cancellationToken).AsTask())
            .ToListAsync(cancellationToken: cancellationToken);

        var finishedTask = await Task.WhenAny(tasks);
        await Task.WhenAll(tasks);
        await finishedTask;
    }

    private async IAsyncEnumerable<ITracker> CreateTrackers(
        IEnumerable<string> urls,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        foreach (var url in urls)
        {
            var uri = Uri.CreateOrNull(url);

            if (uri == null)
                continue;

            var trackers = uri.Scheme switch
            {
                "http" or "https" =>
                [
                    new HttpTracker(
                        p2PClient,
                        httpClient,
                        peerId,
                        metaInfo.Info.InfoHash,
                        url,
                        logger,
                        trackersChannel,
                        forcedIp
                    ),
                ],
                "udp" => await CreateUdpTrackers(uri, cancellationToken),
                _ => LogUnknownTracker(url),
            };

            foreach (var tracker in trackers)
            {
                if (tracker is null)
                    continue;

                yield return tracker;
            }
        }
    }

    private async Task<UdpTracker[]> CreateUdpTrackers(Uri uri, CancellationToken cancellationToken)
    {
        List<UdpTracker> udpTrackers = [];
        var ips = await Dns.GetHostAdressesOrEmptyAsync(uri.Host, cancellationToken);
        var ipv4 = ips.FirstOrDefault(i => i.AddressFamily == AddressFamily.InterNetwork);
        var ipv6 = ips.FirstOrDefault(i => i.AddressFamily == AddressFamily.InterNetworkV6);

        if (ipv4 != default)
        {
            var ipEndpoint = new IPEndPoint(ipv4, uri.Port);
            var trackerv4 = new UdpTracker(
                trackerTransaction,
                p2PClient,
                peerId,
                trackersChannel,
                metaInfo.Info.InfoHash,
                uri.OriginalString,
                ipEndpoint,
                logger,
                forcedIp
            );
            udpTrackers.Add(trackerv4);
        }

        if (ipv6 != default)
        {
            var ipEndpoint = new IPEndPoint(ipv6, uri.Port);
            var trackerv6 = new UdpTracker(
                trackerTransaction,
                p2PClient,
                peerId,
                trackersChannel,
                metaInfo.Info.InfoHash,
                uri.OriginalString,
                ipEndpoint,
                logger,
                forcedIp
            );
            udpTrackers.Add(trackerv6);
        }

        return [.. udpTrackers];
    }

    private ITracker[] LogUnknownTracker(string scheme)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug("Unknown {scheme} tracker", scheme);

        return [];
    }

    public async ValueTask DisposeAsync()
    {
        trackersChannel.TryComplete();
        foreach (var tracker in _trackers)
        {
            await tracker.DisposeAsync();
        }
    }
}
