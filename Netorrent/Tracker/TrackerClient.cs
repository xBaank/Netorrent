using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.P2P.Messages;
using Netorrent.Statistics;
using Netorrent.TorrentFile.FileStructure;
using Netorrent.TorrentFile.Options;
using Netorrent.Tracker.Http;
using Netorrent.Tracker.Udp;
using ZLinq;

namespace Netorrent.Tracker;

internal class TrackerClient(
    TrackerHandlers trackerHandlers,
    UsedTrackers usedTrackers,
    int port,
    DataStatistics transferStatistics,
    PeerId peerId,
    ChannelWriter<IPEndPoint> trackersChannel,
    string[] announceList,
    InfoHash infoHash,
    ILogger logger
) : IAsyncDisposable
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var urls =
            announceList
                .Where(url => !string.IsNullOrWhiteSpace(url))
                .Distinct(StringComparer.OrdinalIgnoreCase)
            ?? [];

        List<Task> trackerTasks = [];
        List<ITracker> trackers = [];
        var trackersEnumerable = CreateTrackers(urls, cancellationToken).ConfigureAwait(false);

        //The trackers should not fail by them self
        //They finish successfully because of dns problems, udp timeouts, etc.
        try
        {
            await foreach (var tracker in trackersEnumerable)
            {
                trackers.Add(tracker);
                trackerTasks.Add(tracker.StartAsync(cancellationToken).AsTask());
            }

            await Task.WhenAll(trackerTasks).ConfigureAwait(false);
        }
        finally
        {
            var trackerDisposeTasks = trackers.Select(i => i.DisposeAsync().AsTask());
            await Task.WhenAll(trackerDisposeTasks).ConfigureAwait(false);
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
            {
                continue;
            }

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
                {
                    continue;
                }

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

        if (trackerHandlers.HttpTrackerHandlerIpv4 is not null)
        {
            var trackerv4 = new HttpTracker(
                port,
                transferStatistics,
                trackerHandlers.HttpTrackerHandlerIpv4,
                peerId,
                infoHash,
                uri.OriginalString,
                logger,
                trackersChannel
            );
            httpsTrackers.Add(trackerv4);
        }

        if (trackerHandlers.HttpTrackerHandlerIpv6 is not null)
        {
            var trackerv6 = new HttpTracker(
                port,
                transferStatistics,
                trackerHandlers.HttpTrackerHandlerIpv6,
                peerId,
                infoHash,
                uri.OriginalString,
                logger,
                trackersChannel
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

        if (trackerHandlers.UdpTrackerHandlerIpv4 is not null && ipv4 is not null && uri.Port > 0)
        {
            var ipEndpoint = new IPEndPoint(ipv4, uri.Port);
            var trackerv4 = new UdpTracker(
                trackerHandlers.UdpTrackerHandlerIpv4,
                port,
                transferStatistics,
                peerId,
                trackersChannel,
                infoHash,
                uri.OriginalString,
                ipEndpoint,
                logger
            );
            udpTrackers.Add(trackerv4);
        }

        if (trackerHandlers.UdpTrackerHandlerIpv6 is not null && ipv6 is not null && uri.Port > 0)
        {
            var ipEndpoint = new IPEndPoint(ipv6, uri.Port);
            var trackerv6 = new UdpTracker(
                trackerHandlers.UdpTrackerHandlerIpv6,
                port,
                transferStatistics,
                peerId,
                trackersChannel,
                infoHash,
                uri.OriginalString,
                ipEndpoint,
                logger
            );
            udpTrackers.Add(trackerv6);
        }

        return [.. udpTrackers];
    }

    private ITracker[] LogUnknownTracker(string scheme)
    {
        if (logger.IsEnabled(LogLevel.Debug))
        {
            logger.LogDebug("Unknown {scheme} tracker", scheme);
        }

        return [];
    }

    public async ValueTask DisposeAsync()
    {
        trackersChannel.TryComplete();
    }
}
