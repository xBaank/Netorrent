using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Netorrent.Bencoding;
using Netorrent.IO;
using Netorrent.P2P;
using Netorrent.P2P.Managers;
using Netorrent.P2P.Messages;
using Netorrent.TorrentFile.FileStructure;
using Netorrent.Tracker;

namespace Netorrent.TorrentFile;

public class Torrent : IAsyncDisposable
{
    public MetaInfo MetaInfo { get; init; }
    public Bitfield Bitfield => _myBitfield;

    private readonly HttpClient _httpClient;
    private readonly P2PClient _p2pClient;
    private readonly List<TrackerClient> _trackerClients;
    private readonly FileManager _fileManager;
    private readonly Bitfield _myBitfield;
    private readonly string _peerId;
    private readonly ILogger _logger;

    public DownloadInfo DownloadInfo => _p2pClient.DownloadInfo;

    internal Torrent(
        MetaInfo metaInfo,
        HttpClient httpClient,
        string peerId,
        string outputDirectory,
        ILogger logger,
        bool bitfieldInitialized = false
    )
    {
        MetaInfo = metaInfo;
        _myBitfield = new Bitfield(metaInfo.Info.Pieces.Length / 20, bitfieldInitialized);
        _fileManager = new FileManager(
            Path.Combine(outputDirectory, MetaInfo.Title ?? ""),
            metaInfo.Info.NormalizedFiles(),
            (int)metaInfo.Info.PieceLength,
            [.. metaInfo.Info.Pieces.Chunk(20)],
            _myBitfield
        );
        _logger = logger;
        _p2pClient = new P2PClient(metaInfo, peerId, _fileManager, _myBitfield, _logger);
        _httpClient = httpClient;
        _peerId = peerId;
        _trackerClients = [];
    }

    public void Start(CancellationToken cancellationToken = default)
    {
        _p2pClient.ListenForPeers(cancellationToken);
        _p2pClient.ListenerTask?.ContinueWith(
            task =>
            {
                if (task.IsFaulted)
                {
                    DownloadInfo.SetException(task.Exception);
                }
            },
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default
        );
        cancellationToken.Register(() => _p2pClient.DownloadInfo.SetCanceled());

        var trackers = MetaInfo
            .AnnounceList?.Append(MetaInfo.Announce)
            .Where(url => url.StartsWith("http://") || url.StartsWith("https://"))
            .Distinct()
            ?.Select(url => new TrackerClient(
                _p2pClient,
                _httpClient,
                _peerId,
                MetaInfo.Info.InfoHash,
                url,
                _logger
            ))
            .ToList();

        if (trackers is null || trackers.Count == 0)
            throw new InvalidOperationException("No supported tracker URLs found.");

        foreach (var tracker in trackers)
        {
            tracker.Start(cancellationToken);
            tracker.TrackerTask.ContinueWith(
                async task =>
                {
                    if (task.IsFaulted)
                    {
                        DownloadInfo.SetException(task.Exception);
                    }
                    await tracker.DisposeAsync();
                },
                CancellationToken.None,
                TaskContinuationOptions.RunContinuationsAsynchronously,
                TaskScheduler.Default
            );
        }
    }

    public async Task ExportAsync(string outputPath, CancellationToken cancellationToken = default)
    {
        var folder = Path.GetDirectoryName(outputPath);

        if (folder is not null)
            Directory.CreateDirectory(folder);

        var rawMetainfo = MetaInfo.ToBDictionary();
        await using var encoder = new BEncoder();
        await File.WriteAllBytesAsync(outputPath, encoder.Encode(rawMetainfo), cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        _fileManager.Dispose();
        await _p2pClient.DisposeAsync();
        foreach (var item in _trackerClients)
        {
            await item.DisposeAsync();
        }
    }
}
