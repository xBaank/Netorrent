using System.Net;
using Netorrent.TorrentFile;
using Netorrent.TorrentFile.FileStructure;
using Netorrent.Tracker.Http;

namespace Netorrent.Tests.Fakes;

internal class FakeHttpTrackerHandler(IPEndPoint[] ips, TimeSpan interval, Exception? error = null)
    : IHttpTrackerHandler
{
    public void Dispose() { }

    public ValueTask<HttpTrackerResponse> SendAsync(
        string url,
        HttpTrackerRequest httpTrackerRequest,
        CancellationToken cancellationToken
    ) =>
        error is null
            ? ValueTask.FromResult(
                new HttpTrackerResponse
                {
                    Interval = (int)interval.TotalSeconds,
                    Complete = ips.Length,
                    Incomplete = 0,
                    Peers = [.. ips],
                }
            )
            : ValueTask.FromException<HttpTrackerResponse>(error);

    public ValueTask<ScrapeInfo?> ScrapeAsync(
        string announceUrl,
        InfoHash infoHash,
        CancellationToken cancellationToken
    ) =>
        error is null
            ? ValueTask.FromResult<ScrapeInfo?>(new ScrapeInfo(ips.Length, 0, 0))
            : ValueTask.FromException<ScrapeInfo?>(error);
}
