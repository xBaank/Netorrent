using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.P2P.Messages;
using Netorrent.Statistics;
using Netorrent.TorrentFile;
using Netorrent.Tracker.Http;
using Netorrent.Tracker.Udp;
using ZLinq;

namespace Netorrent.Tracker;

internal class TrackerClient(
    IHttpTrackerHandler httpTrackerHandler,
    IUdpTrackerTransactionManager trackerTransactionManager,
    UsedAdressProtocol usedAdressProtocol,
    UsedTrackers usedTrackers,
    int port,
    TransferStatistics transferStatistics,
    PeerId peerId,
    ChannelWriter<IPEndPoint> trackersChannel,
    string[] announceList,
    byte[] infoHash,
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
        var tasks = await CreateTrackers(urls, cancellationToken)
            .Select(i => i.StartAsync(cancellationToken).AsTask())
            .ToListAsync(cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        await Task.WhenAll(tasks).ConfigureAwait(false);
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
                [
                    new HttpTracker(
                        port,
                        transferStatistics,
                        httpTrackerHandler,
                        peerId,
                        infoHash,
                        url,
                        logger,
                        trackersChannel,
                        forcedIp
                    ),
                ],
                "udp" when usedTrackers.HasFlag(UsedTrackers.Udp) => await CreateUdpTrackers(
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

    private async Task<UdpTracker[]> CreateUdpTrackers(Uri uri, CancellationToken cancellationToken)
    {
        List<UdpTracker> udpTrackers = [];
        var ips = await Dns.GetHostAdressesOrEmptyAsync(uri.Host, cancellationToken)
            .ConfigureAwait(false);
        var ipv4 = ips.AsValueEnumerable()
            .FirstOrDefault(i => i.AddressFamily == AddressFamily.InterNetwork);
        var ipv6 = ips.AsValueEnumerable()
            .FirstOrDefault(i => i.AddressFamily == AddressFamily.InterNetworkV6);
        var supportedAdressFamilies = usedAdressProtocol.ToAddressFamily();

        if (
            supportedAdressFamilies.Contains(AddressFamily.InterNetwork)
            && ipv4 != default
            && uri.Port > 0
        )
        {
            var ipEndpoint = new IPEndPoint(ipv4, uri.Port);
            var trackerv4 = new UdpTracker(
                trackerTransactionManager,
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
            supportedAdressFamilies.Contains(AddressFamily.InterNetworkV6)
            && ipv6 != default
            && uri.Port > 0
        )
        {
            var ipEndpoint = new IPEndPoint(ipv6, uri.Port);
            var trackerv6 = new UdpTracker(
                trackerTransactionManager,
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
