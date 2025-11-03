using Netorrent.IO;
using Netorrent.P2P;
using Netorrent.P2P.Messages;
using Netorrent.TorrentFile.FileStructure;
using Netorrent.Tracker;

namespace Netorrent.TorrentFile;

public class Torrent
{
    public MetaInfo MetaInfo { get; init; }

    private readonly HttpClient _httpClient;
    private readonly P2PClient _p2pClient;
    private readonly FileManager _fileManager;
    private readonly string _peerId;

    internal Torrent(
        MetaInfo metaInfo,
        HttpClient httpClient,
        string peerId,
        string outputDirectory,
        bool bitfieldInitialized = false
    )
    {
        MetaInfo = metaInfo;
        var bitField = new Bitfield(metaInfo.Info.Pieces.Length / 20, bitfieldInitialized);
        _fileManager = new FileManager(
            outputDirectory,
            metaInfo.Info.NormalizedFiles(),
            (int)metaInfo.Info.PieceLength,
            [.. metaInfo.Info.Pieces.Chunk(20)],
            bitField
        );
        _p2pClient = new P2PClient(metaInfo, peerId, _fileManager, bitField);
        _httpClient = httpClient;
        _peerId = peerId;
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        var trackerClients = MetaInfo
            .AnnounceList?.Append(MetaInfo.Announce)
            .Where(url => url.StartsWith("http://") || url.StartsWith("https://"))
            .Distinct()
            ?.Select(url => new TrackerClient(
                _p2pClient,
                _httpClient,
                _peerId,
                MetaInfo.Info.InfoHash,
                url
            ))
            .ToList();

        if (trackerClients is null || trackerClients.Count == 0)
            throw new InvalidOperationException("No supported tracker URLs found.");

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
