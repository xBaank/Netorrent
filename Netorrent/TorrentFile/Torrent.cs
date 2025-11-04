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
    private readonly Bitfield _myBitfield;
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
        _myBitfield = new Bitfield(metaInfo.Info.Pieces.Length / 20, bitfieldInitialized);
        _fileManager = new FileManager(
            outputDirectory,
            metaInfo.Info.NormalizedFiles(),
            (int)metaInfo.Info.PieceLength,
            [.. metaInfo.Info.Pieces.Chunk(20)],
            _myBitfield
        );
        _p2pClient = new P2PClient(metaInfo, peerId, _fileManager, _myBitfield);
        _httpClient = httpClient;
        _peerId = peerId;
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        _p2pClient.ListenForPeers(cancellationToken);

        var trackers = MetaInfo
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

        if (trackers is null || trackers.Count == 0)
            throw new InvalidOperationException("No supported tracker URLs found.");

        try
        {
            foreach (var tracker in trackers)
            {
                tracker.Start(cancellationToken);
                await tracker.TrackerTask;
            }
        }
        finally
        {
            foreach (var tracker in trackers)
            {
                await tracker.DisposeAsync();
            }
        }
    }
}
