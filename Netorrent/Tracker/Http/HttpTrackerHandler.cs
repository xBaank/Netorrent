using System.Text;
using Netorrent.Bencoding;
using Netorrent.Bencoding.Structs;
using Netorrent.Extensions;
using Netorrent.TorrentFile;
using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.Tracker.Http;

internal class HttpTrackerHandler(HttpClient httpClient) : IHttpTrackerHandler
{
    public async ValueTask<HttpTrackerResponse> SendAsync(
        string url,
        HttpTrackerRequest httpTrackerRequest,
        CancellationToken cancellationToken
    )
    {
        var response = await httpClient
            .SendAsync(httpTrackerRequest.GenerateRequest(url), cancellationToken)
            .ConfigureAwait(false);

        return await HttpTrackerResponse
            .FromHttpResponseAsync(response, cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask<ScrapeInfo?> ScrapeAsync(
        string announceUrl,
        InfoHash infoHash,
        CancellationToken cancellationToken
    )
    {
        var scrapeUrl = GetScrapeUrl(announceUrl);
        if (scrapeUrl is null)
            return null;

        var sb = new StringBuilder(scrapeUrl);
        sb.Append(scrapeUrl.Contains('?') ? '&' : '?');
        sb.Append("info_hash=");
        foreach (var b in infoHash.Data.Span)
        {
            sb.Append('%').Append(b.ToString("X2"));
        }

        var response = await httpClient
            .GetAsync(sb.ToString(), cancellationToken)
            .ConfigureAwait(false);
        response.EnsureSuccessStatusCode();

        var stream = await response
            .Content.ReadAsStreamAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var decoder = new BDecoder(stream);
        var root = await decoder.DecodeAsync(cancellationToken).ConfigureAwait(false);

        if (root.As<BDictionary>() is not { } dict)
            return null;

        if (dict.Elements.GetValueOrDefault("files").As<BDictionary>() is not { } filesDict)
            return null;

        var hashKey = new BString(infoHash.Data.ToArray());
        if (filesDict.Elements.GetValueOrDefault(hashKey).As<BDictionary>() is not { } torrentDict)
            return null;

        var seeders = (int)(
            torrentDict.Elements.GetValueOrDefault("complete").As<BInt>()?.Data ?? 0
        );
        var leechers = (int)(
            torrentDict.Elements.GetValueOrDefault("incomplete").As<BInt>()?.Data ?? 0
        );
        var downloaded = (int)(
            torrentDict.Elements.GetValueOrDefault("downloaded").As<BInt>()?.Data ?? 0
        );

        return new ScrapeInfo(seeders, leechers, downloaded);
    }

    private static string? GetScrapeUrl(string announceUrl)
    {
        if (!announceUrl.Contains("/announce", StringComparison.OrdinalIgnoreCase))
            return null;
        return announceUrl.Replace("/announce", "/scrape", StringComparison.OrdinalIgnoreCase);
    }

    public void Dispose()
    {
        httpClient.Dispose();
    }
}
