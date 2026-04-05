using System.Net;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Exceptions;
using Netorrent.Extensions;
using Netorrent.P2P.Messages;
using Netorrent.Statistics;
using Netorrent.TorrentFile;
using Netorrent.TorrentFile.FileStructure;
using Netorrent.TorrentFile.Options;
using Netorrent.Tracker.Http;
using Netorrent.Tracker.Udp;
using ZLinq;

namespace Netorrent.Tracker;

internal class TrackerClient(
    Bitfield myBitfield,
    TrackerHandlers trackerHandlers,
    UsedTrackers usedTrackers,
    int port,
    DataStatistics transferStatistics,
    PeerId peerId,
    ChannelWriter<IPEndPoint> trackersChannel,
    List<string[]> announceList,
    InfoHash infoHash,
    ILogger logger
) : IAsyncDisposable
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var urls in announceList)
        {
            var snapshot = urls.AsValueEnumerable().Shuffle().ToArray();

            await foreach (
                var (url, (Ipv4, Ipv6)) in CreateTrackers(snapshot, cancellationToken)
                    .WithCancellation(cancellationToken)
                    .ConfigureAwait(false)
            )
            {
                try
                {
                    Task?[] tasks =
                    [
                        Ipv4?.StartAsync(cancellationToken).AsTask(),
                        Ipv6?.StartAsync(cancellationToken).AsTask(),
                    ];

                    await Task.WhenAll(tasks.Where(i => i is not null).Cast<Task>())
                        .ConfigureAwait(false);
                }
                catch (AnnounceException ex)
                {
                    if (logger.IsEnabled(LogLevel.Error))
                    {
                        logger.LogError(ex, "Error Announcing");
                    }

                    if (Ipv4 is not null)
                        await Ipv4.DisposeAsync().ConfigureAwait(false);
                    if (Ipv6 is not null)
                        await Ipv6.DisposeAsync().ConfigureAwait(false);
                }
                catch (OperationCanceledException oce)
                    when (oce.CancellationToken == cancellationToken)
                {
                    try
                    {
                        using var ct = new CancellationTokenSource(5.Seconds);
                        if (Ipv4 is not null)
                        {
                            await Ipv4.StopAsync(ct.Token).ConfigureAwait(false);
                            await Ipv4.DisposeAsync().ConfigureAwait(false);
                        }
                        if (Ipv6 is not null)
                        {
                            await Ipv6.StopAsync(ct.Token).ConfigureAwait(false);
                            await Ipv6.DisposeAsync().ConfigureAwait(false);
                        }

                        Promote(urls, url);
                    }
                    catch (Exception stopEx)
                    {
                        if (logger.IsEnabled(LogLevel.Error))
                        {
                            logger.LogError(stopEx, "Error stopping");
                        }
                    }

                    return;
                }
            }
        }
    }

    private static void Promote(string[] tier, string winner)
    {
        var idx = Array.IndexOf(tier, winner);
        if (idx <= 0)
            return;

        // Swap to front
        var first = tier[0];
        tier[0] = winner;
        tier[idx] = first;
    }

    private async IAsyncEnumerable<(string url, (ITracker? Ipv4, ITracker? Ipv6))> CreateTrackers(
        string[] urls,
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

            yield return (url, trackers);
        }
    }

    private async ValueTask<(HttpTracker? Ipv4, HttpTracker? Ipv6)> CreateHttpTrackersAsync(
        Uri uri,
        CancellationToken cancellationToken
    )
    {
        List<HttpTracker> httpsTrackers = [];
        var (ipv4, ipv6) = await Dns.GetHostAdressesOrEmptyAsync(uri, cancellationToken)
            .ConfigureAwait(false);

        HttpTracker? trackerv4 = null;
        HttpTracker? trackerv6 = null;

        if (trackerHandlers.HttpTrackerHandlerIpv4 is not null && ipv4 is not null)
        {
            trackerv4 = new HttpTracker(
                myBitfield,
                port,
                transferStatistics,
                trackerHandlers.HttpTrackerHandlerIpv4,
                peerId,
                infoHash,
                uri.OriginalString,
                trackersChannel
            );
            httpsTrackers.Add(trackerv4);
        }

        if (trackerHandlers.HttpTrackerHandlerIpv6 is not null && ipv6 is not null)
        {
            trackerv6 = new HttpTracker(
                myBitfield,
                port,
                transferStatistics,
                trackerHandlers.HttpTrackerHandlerIpv6,
                peerId,
                infoHash,
                uri.OriginalString,
                trackersChannel
            );
            httpsTrackers.Add(trackerv6);
        }

        return (trackerv4, trackerv6);
    }

    private async ValueTask<(UdpTracker? Ipv4, UdpTracker? Ipv6)> CreateUdpTrackersAsync(
        Uri uri,
        CancellationToken cancellationToken
    )
    {
        List<UdpTracker> udpTrackers = [];
        var (ipv4, ipv6) = await Dns.GetHostAdressesOrEmptyAsync(uri, cancellationToken)
            .ConfigureAwait(false);

        UdpTracker? trackerv4 = null;
        UdpTracker? trackerv6 = null;

        if (trackerHandlers.UdpTrackerHandlerIpv4 is not null && ipv4 is not null && uri.Port > 0)
        {
            var ipEndpoint = new IPEndPoint(ipv4, uri.Port);
            trackerv4 = new UdpTracker(
                myBitfield,
                trackerHandlers.UdpTrackerHandlerIpv4,
                port,
                transferStatistics,
                peerId,
                trackersChannel,
                infoHash,
                ipEndpoint
            );
            udpTrackers.Add(trackerv4);
        }

        if (trackerHandlers.UdpTrackerHandlerIpv6 is not null && ipv6 is not null && uri.Port > 0)
        {
            var ipEndpoint = new IPEndPoint(ipv6, uri.Port);
            trackerv6 = new UdpTracker(
                myBitfield,
                trackerHandlers.UdpTrackerHandlerIpv6,
                port,
                transferStatistics,
                peerId,
                trackersChannel,
                infoHash,
                ipEndpoint
            );
            udpTrackers.Add(trackerv6);
        }

        return (trackerv4, trackerv6);
    }

    public async ValueTask<ScrapeInfo?> ScrapeAsync(CancellationToken cancellationToken)
    {
        foreach (var url in announceList.SelectMany(urls => urls))
        {
            var uri = Uri.CreateOrNull(url);
            if (uri is null)
                continue;

            try
            {
                var result = await ScrapeSingleAsync(uri, url, cancellationToken)
                    .ConfigureAwait(false);
                if (result is not null)
                    return result;
            }
            catch (Exception ex)
            {
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug(ex, "Scrape failed for {url}", url);
            }
        }

        return null;
    }

    private async ValueTask<ScrapeInfo?> ScrapeSingleAsync(
        Uri uri,
        string url,
        CancellationToken cancellationToken
    ) =>
        uri.Scheme switch
        {
            "http"
            or "https"
                when usedTrackers.HasFlag(UsedTrackers.Http)
                    && trackerHandlers.HttpTrackerHandlerIpv4 is not null => await trackerHandlers
                .HttpTrackerHandlerIpv4.ScrapeAsync(url, infoHash, cancellationToken)
                .ConfigureAwait(false),
            "udp"
                when usedTrackers.HasFlag(UsedTrackers.Udp)
                    && trackerHandlers.UdpTrackerHandlerIpv4 is not null
                    && uri.Port > 0 => await ScrapeUdpAsync(
                    uri,
                    trackerHandlers.UdpTrackerHandlerIpv4,
                    cancellationToken
                )
                .ConfigureAwait(false),
            _ => null,
        };

    private async ValueTask<ScrapeInfo?> ScrapeUdpAsync(
        Uri uri,
        IUdpTrackerHandler handler,
        CancellationToken cancellationToken
    )
    {
        var (ipv4, _) = await Dns.GetHostAdressesOrEmptyAsync(uri, cancellationToken)
            .ConfigureAwait(false);
        if (ipv4 is null)
            return null;

        var endPoint = new IPEndPoint(ipv4, uri.Port);
        return await handler
            .ScrapeAsync(endPoint, infoHash, cancellationToken)
            .ConfigureAwait(false);
    }

    private (ITracker? Ipv4, ITracker? Ipv6) LogUnknownTracker(string scheme)
    {
        if (logger.IsEnabled(LogLevel.Debug))
        {
            logger.LogDebug("Unknown {scheme} tracker", scheme);
        }

        return (null, null);
    }

    public async ValueTask DisposeAsync()
    {
        trackersChannel.TryComplete();
    }
}
