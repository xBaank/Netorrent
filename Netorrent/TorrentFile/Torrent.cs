using Netorrent.IO;
using Netorrent.P2P;
using Netorrent.TorrentFile.FileStructure;
using Netorrent.Tracker;

namespace Netorrent.TorrentFile;

public class Torrent
{
    public MetaInfo MetaInfo { get; init; }

    private readonly HttpClient _httpClient;
    private readonly P2PClient _p2pClient;
    private readonly string _peerId;

    private readonly List<TrackerClient> _trackerClients = [];

    internal Torrent(MetaInfo metaInfo, HttpClient httpClient, string peerId)
    {
        MetaInfo = metaInfo;
        _p2pClient = new P2PClient(MetaInfo, peerId);
        _httpClient = httpClient;
        _peerId = peerId;
    }

    public async ValueTask StartAsync(CancellationToken cancellationToken = default)
    {
        var trackerClients = MetaInfo
            .AnnounceList?.Append(MetaInfo.Announce)
            .Where(url => url.StartsWith("http://") || url.StartsWith("https://"))
            ?.Select(url => new TrackerClient(
                _p2pClient,
                _httpClient,
                _peerId,
                MetaInfo.Info.InfoHash,
                url,
                new FilesHandler(MetaInfo)
            ))
            .ToList();

        if (trackerClients is null || trackerClients.Count == 0)
            throw new InvalidOperationException("No supported tracker URLs found.");

        _trackerClients.AddRange(trackerClients);

        try
        {
            var trakersTasks = trackerClients
                .Select(client => client.StartAsync(cancellationToken))
                .ToList();

            await Task.WhenAll(trakersTasks);
        }
        finally
        {
            foreach (var client in trackerClients)
            {
                await client.DisposeAsync();
            }
        }
    }
}
