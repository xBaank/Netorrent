using System.Net.Http;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.P2P;
using Netorrent.TorrentFile.FileStructure;
using Netorrent.Tracker.Http;

namespace Netorrent.Tracker;

internal class TrackerClient(
    HttpClient httpClient,
    P2PClient p2PClient,
    string peerId,
    ChannelWriter<HttpTrackerResponse> trackersChannel,
    MetaInfo metaInfo,
    ILogger logger
) : IAsyncDisposable
{
    private List<HttpTracker> _trackers = [];

    public void Start(CancellationToken cancellationToken)
    {
        _trackers =
            metaInfo
                .AnnounceList?.Append(metaInfo.Announce)
                .Where(url => url.StartsWith("http://") || url.StartsWith("https://"))
                .Distinct()
                ?.Select(url => new HttpTracker(
                    p2PClient,
                    httpClient,
                    peerId,
                    metaInfo.Info.InfoHash,
                    url,
                    logger,
                    trackersChannel
                ))
                .ToList() ?? [];

        foreach (var tracker in _trackers)
        {
            tracker.Start(cancellationToken);
            tracker.TrackerTask?.ContinueWith(
                async task =>
                {
                    if (task.IsFaulted)
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

    public async ValueTask DisposeAsync()
    {
        trackersChannel.TryComplete();
        foreach (var tracker in _trackers)
        {
            await tracker.DisposeAsync();
        }
    }
}
