using System.Net;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Bencoding;
using Netorrent.IO;
using Netorrent.P2P;
using Netorrent.P2P.Messages;
using Netorrent.TorrentFile.FileStructure;
using Netorrent.Tracker;
using Netorrent.Tracker.Udp;

namespace Netorrent.TorrentFile;

public class Torrent : IAsyncDisposable
{
    public MetaInfo MetaInfo { get; init; }
    public Bitfield Bitfield => _myBitfield;
    public DownloadInfo DownloadInfo => _p2pClient.DownloadInfo;

    public State State { get; private set; } = State.None;

    private readonly P2PClient _p2pClient;
    private readonly TrackerClient _trackerClient;
    private readonly FileManager _fileManager;
    private readonly Bitfield _myBitfield;
    private readonly Func<IPAddress, IPAddress>? PeerIpProxy;
    private CancellationTokenSource? _cancellationTokenSource;

    internal Torrent(
        MetaInfo metaInfo,
        HttpClient httpClient,
        UdpTrackerTransactionManager trackerTransaction,
        PeerId peerId,
        string outputDirectory,
        ILogger logger,
        IPAddress? forcedIp = null,
        bool bitfieldInitialized = false,
        Func<IPAddress, IPAddress>? peerIpProxy = null
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
        var trackersChannel = Channel.CreateBounded<IPEndPoint>(
            new BoundedChannelOptions(100) { SingleWriter = false, SingleReader = true }
        );
        _p2pClient = new P2PClient(
            metaInfo,
            peerId,
            _fileManager,
            _myBitfield,
            trackersChannel.Reader,
            logger,
            peerIpProxy
        );
        _trackerClient = new TrackerClient(
            httpClient,
            trackerTransaction,
            _p2pClient,
            peerId,
            trackersChannel.Writer,
            metaInfo,
            logger,
            forcedIp
        );
        PeerIpProxy = peerIpProxy;
    }

    public void Start()
    {
        if (State == State.Started)
            return;

        DownloadInfo.Reset();
        _cancellationTokenSource = new();
        var cancellationToken = _cancellationTokenSource.Token;

        _p2pClient.ListenForPeers(cancellationToken);
        _p2pClient.ProcessPeers(cancellationToken);
        _p2pClient.ListenerTask?.ContinueWith(
            task =>
            {
                if (task.IsCanceled)
                {
                    DownloadInfo.SetCanceled();
                }
                else if (task.IsFaulted)
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
                if (task.IsCanceled)
                {
                    DownloadInfo.SetCanceled();
                }
                else if (task.IsFaulted)
                {
                    DownloadInfo.SetException(task.Exception);
                }
            },
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default
        );

        cancellationToken.Register(_p2pClient.DownloadInfo.SetCanceled);
        _trackerClient.Start(cancellationToken);

        State = State.Started;
    }

    public void Stop()
    {
        if (State != State.Started)
            return;

        _cancellationTokenSource?.Cancel();
        State = State.Stopped;
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
        await _trackerClient.DisposeAsync();
    }
}
