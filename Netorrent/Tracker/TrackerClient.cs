using System.Net;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.P2P;
using Netorrent.TorrentFile.FileStructure;
using Netorrent.Tracker.Http;
using Netorrent.Tracker.Udp;

namespace Netorrent.Tracker;

internal class TrackerClient(
    HttpClient httpClient,
    UdpTrackerTransactionManager trackerTransaction,
    P2PClient p2PClient,
    string peerId,
    ChannelWriter<IPEndPoint> trackersChannel,
    MetaInfo metaInfo,
    ILogger logger
) : IAsyncDisposable
{
    private List<ITracker> _trackers = [];

    public void Start(CancellationToken cancellationToken)
    {
        _trackers =
            metaInfo
                .AnnounceList?.Append(metaInfo.Announce)
                .Distinct()
                ?.Select(CreateTracker)
                .Where(i => i != null)
                .Cast<ITracker>()
                .ToList() ?? [];

        foreach (var tracker in _trackers)
        {
            tracker.Start(cancellationToken);
            tracker.TrackerTask?.ContinueWith(
                async task =>
                {
                    if (task.IsCanceled)
                    {
                        p2PClient.DownloadInfo.SetCanceled();
                    }
                    else if (task.IsFaulted)
                    {
                        p2PClient.DownloadInfo.SetException(task.Exception);
                    }
                    await tracker.DisposeAsync();
                },
                CancellationToken.None,
                TaskContinuationOptions.RunContinuationsAsynchronously,
                TaskScheduler.Default
            );
        }
    }

    private ITracker? CreateTracker(string url) =>
        new Uri(url).Scheme switch
        {
            "http" or "https" => new HttpTracker(
                p2PClient,
                httpClient,
                peerId,
                metaInfo.Info.InfoHash,
                url,
                logger,
                trackersChannel
            ),
            "udp" => new UdpTracker(
                trackerTransaction,
                p2PClient,
                peerId,
                trackersChannel,
                metaInfo.Info.InfoHash,
                url,
                logger
            ),
            _ => LogUnknownTracker(url),
        };

    private ITracker? LogUnknownTracker(string scheme)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug("Unknown {scheme} tracker", scheme);
        return null;
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
