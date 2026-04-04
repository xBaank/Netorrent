using System.Text;
using Netorrent.Bencoding;
using Netorrent.Bencoding.Structs;
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

        if (root is not BDictionary dict)
            return null;

        if (
            !dict.Elements.TryGetValue("files", out var filesNode)
            || filesNode is not BDictionary filesDict
        )
            return null;

        var hashKey = new BString(infoHash.Data.ToArray());
        if (
            !filesDict.Elements.TryGetValue(hashKey, out var torrentNode)
            || torrentNode is not BDictionary torrentDict
        )
            return null;

        var seeders = torrentDict.Elements.TryGetValue("complete", out var comp)
            ? (int)((BInt)comp).Data
            : 0;
        var leechers = torrentDict.Elements.TryGetValue("incomplete", out var incomp)
            ? (int)((BInt)incomp).Data
            : 0;
        var downloaded = torrentDict.Elements.TryGetValue("downloaded", out var dl)
            ? (int)((BInt)dl).Data
            : 0;

        return new ScrapeInfo(seeders, leechers, downloaded);
    }

    private static string? GetScrapeUrl(string announceUrl)
    {
        var idx = announceUrl.LastIndexOf("/announce", StringComparison.OrdinalIgnoreCase);
        if (idx < 0)
            return null;
        return announceUrl[..idx] + "/scrape" + announceUrl[(idx + "/announce".Length)..];
    }

    public void Dispose()
    {
        httpClient.Dispose();
    }
}
