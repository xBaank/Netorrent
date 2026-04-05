using Netorrent.TorrentFile;
using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.Tracker.Http;

internal interface IHttpTrackerHandler : IDisposable
{
    ValueTask<HttpTrackerResponse> SendAsync(
        string url,
        HttpTrackerRequest httpTrackerRequest,
        CancellationToken cancellationToken
    );

    ValueTask<ScrapeInfo?> ScrapeAsync(
        string announceUrl,
        InfoHash infoHash,
        CancellationToken cancellationToken
    );
}
