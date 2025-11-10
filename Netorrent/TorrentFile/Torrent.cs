using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Bencoding;
using Netorrent.IO;
using Netorrent.P2P;
using Netorrent.P2P.Managers;
using Netorrent.P2P.Messages;
using Netorrent.TorrentFile.FileStructure;
using Netorrent.Tracker;
using Netorrent.Tracker.Http;

namespace Netorrent.TorrentFile;

public class Torrent : IAsyncDisposable
{
    public MetaInfo MetaInfo { get; init; }
    public Bitfield Bitfield => _myBitfield;

    private readonly P2PClient _p2pClient;
    private readonly TrackerClient _trackerClient;
    private readonly FileManager _fileManager;
    private readonly Bitfield _myBitfield;
    private bool _disposed = false;

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
        var trackersChannel = Channel.CreateBounded<HttpTrackerResponse>(
            new BoundedChannelOptions(50) { SingleWriter = false, SingleReader = true }
        );
        _p2pClient = new P2PClient(
            metaInfo,
            peerId,
            _fileManager,
            _myBitfield,
            trackersChannel.Reader,
            logger
        );
        _trackerClient = new TrackerClient(
            httpClient,
            _p2pClient,
            peerId,
            trackersChannel.Writer,
            metaInfo,
            logger
        );
    }

    public void Start(CancellationToken cancellationToken = default)
    {
        _p2pClient.ListenForPeers(cancellationToken);
        _p2pClient.ProcessPeers(cancellationToken);
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
        _p2pClient.PeersTask?.ContinueWith(
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
        _trackerClient.Start(cancellationToken);
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

    protected virtual void Dispose(bool disposing)
    {
        if (_disposed)
            return;

        if (disposing)
        {
            _fileManager.Dispose();
        }

        _disposed = true;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        if (_p2pClient is not null)
            await _p2pClient.DisposeAsync();

        if (_trackerClient is not null)
            await _trackerClient.DisposeAsync();

        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    ~Torrent()
    {
        Dispose(disposing: false);
    }
}
