using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.P2P.Messages;
using Netorrent.Statistics;
using Netorrent.TorrentFile;
using Netorrent.TorrentFile.FileStructure;
using Netorrent.Tracker.Http;
using Netorrent.Tracker.Udp;
using ZLinq;

namespace Netorrent.Tracker;

internal class TrackerClient(
    IHttpTrackerHandler httpTrackerHandler,
    IUdpTrackerHandler udpTrackerHandler,
    IReadOnlySet<AddressFamily> supportedAddressFamilies,
    UsedTrackers usedTrackers,
    int port,
    TransferStatistics transferStatistics,
    PeerId peerId,
    ChannelWriter<IPEndPoint> trackersChannel,
    string[] announceList,
    InfoHash infoHash,
    ILogger logger,
    IPAddress? forcedIp
) : IAsyncDisposable
{
    private readonly List<ITracker> _trackers = [];

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var urls =
            announceList
                .Where(url => !string.IsNullOrWhiteSpace(url))
                .Distinct(StringComparer.OrdinalIgnoreCase)
            ?? [];

        //The trackers should not fail by them self
        //They finish successfully because of dns problems, udp timeouts, etc.

        var trackers = await CreateTrackers(urls, cancellationToken)
            .ToListAsync(cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        try
        {
            var tasks = trackers.Select(i => i.StartAsync(cancellationToken).AsTask());
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
        finally
        {
            foreach (var tracker in trackers)
            {
                await tracker.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private async IAsyncEnumerable<ITracker> CreateTrackers(
        IEnumerable<string> urls,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        foreach (var url in urls)
        {
            var uri = Uri.CreateOrNull(url);

            if (uri is null)
                continue;

            var trackers = uri.Scheme switch
            {
                "http" or "https" when usedTrackers.HasFlag(UsedTrackers.Http) =>
                    await CreateHttpTrackersAsync(uri, cancellationToken).ConfigureAwait(false),
                "udp" when usedTrackers.HasFlag(UsedTrackers.Udp) => await CreateUdpTrackersAsync(
                        uri,
                        cancellationToken
                    )
                    .ConfigureAwait(false),
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

    private async ValueTask<HttpTracker[]> CreateHttpTrackersAsync(
        Uri uri,
        CancellationToken cancellationToken
    )
    {
        List<HttpTracker> httpsTrackers = [];
        var (ipv4, ipv6) = await Dns.GetHostAdressesOrEmptyAsync(uri, cancellationToken)
            .ConfigureAwait(false);

        if (supportedAddressFamilies.Contains(AddressFamily.InterNetwork) && ipv4 is not null)
        {
            var trackerv4 = new HttpTracker(
                port,
                transferStatistics,
                httpTrackerHandler,
                AddressFamily.InterNetwork,
                peerId,
                infoHash,
                uri.OriginalString,
                logger,
                trackersChannel,
                forcedIp
            );
            httpsTrackers.Add(trackerv4);
        }

        if (supportedAddressFamilies.Contains(AddressFamily.InterNetworkV6) && ipv6 is not null)
        {
            var trackerv6 = new HttpTracker(
                port,
                transferStatistics,
                httpTrackerHandler,
                AddressFamily.InterNetworkV6,
                peerId,
                infoHash,
                uri.OriginalString,
                logger,
                trackersChannel,
                forcedIp
            );
            httpsTrackers.Add(trackerv6);
        }

        return [.. httpsTrackers];
    }

    private async ValueTask<UdpTracker[]> CreateUdpTrackersAsync(
        Uri uri,
        CancellationToken cancellationToken
    )
    {
        List<UdpTracker> udpTrackers = [];
        var (ipv4, ipv6) = await Dns.GetHostAdressesOrEmptyAsync(uri, cancellationToken)
            .ConfigureAwait(false);

        if (
            supportedAddressFamilies.Contains(AddressFamily.InterNetwork)
            && ipv4 is not null
            && uri.Port > 0
        )
        {
            var ipEndpoint = new IPEndPoint(ipv4, uri.Port);
            var trackerv4 = new UdpTracker(
                udpTrackerHandler,
                port,
                transferStatistics,
                peerId,
                trackersChannel,
                infoHash,
                uri.OriginalString,
                ipEndpoint,
                logger,
                forcedIp
            );
            udpTrackers.Add(trackerv4);
        }

        if (
            supportedAddressFamilies.Contains(AddressFamily.InterNetworkV6)
            && ipv6 is not null
            && uri.Port > 0
        )
        {
            var ipEndpoint = new IPEndPoint(ipv6, uri.Port);
            var trackerv6 = new UdpTracker(
                udpTrackerHandler,
                port,
                transferStatistics,
                peerId,
                trackersChannel,
                infoHash,
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
            await tracker.DisposeAsync().ConfigureAwait(false);
        }
    }
}
