using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Netorrent.Bencoding;
using Netorrent.Dht;
using Netorrent.Dht.Routing;
using Netorrent.Extensions;
using Netorrent.IO.Disk;
using Netorrent.Other;
using Netorrent.P2P;
using Netorrent.P2P.Download;
using Netorrent.P2P.Messages;
using Netorrent.P2P.Tcp;
using Netorrent.P2P.Upload;
using Netorrent.Statistics;
using Netorrent.TorrentFile.FileStructure;
using Netorrent.TorrentFile.Options;
using Netorrent.Tracker;
using Netorrent.Tracker.Udp.Client;
using ZLinq;

namespace Netorrent.TorrentFile;

public sealed class Torrent : IAsyncDisposable
{
    public MetaInfo MetaInfo { get; init; }
    public CompletionTracker Completion { get; }
    public TorrentStatisticsClient Statistics { get; }
    public string OutputDirectory { get; }
    public State State { get; private set; } = State.Stopped;
    public Bitfield Bitfield => _myBitfield;

    private readonly TcpPeersConnector _peerConnector;
    private readonly TcpPeersListeners _peersListener;
    private readonly PeersClient _peersClient;
    private readonly TrackerClient _trackerClient;
    private readonly DhtClient? _dhtClient;
    private readonly DiskStorage _pieceStorage;
    private readonly Bitfield _myBitfield;
    private readonly PiecePicker _piecePicker;
    private readonly IRequestScheduler _requestScheduler;
    private readonly IUploadScheduler _uploadScheduler;
    private Task? _runTask;
    private CancellationTokenSource? _cancellationTokenSource;
    private bool _disposed;

    internal Torrent(
        MetaInfo metaInfo,
        TrackerHandlers trackerHandlers,
        TcpPeersListeners peersListener,
        PeerId peerId,
        string outputDirectory,
        TorrentClientOptions torrentClientOptions,
        IReadOnlySet<int> downloadedPieces
    )
    {
        var files = metaInfo.Info.NormalizedFiles;
        var totalSize = files.Sum(i => i.Length);
        var trackersChannel = Channel.CreateBounded<IPEndPoint>(
            new BoundedChannelOptions(128) { SingleWriter = false, SingleReader = true }
        );

        MetaInfo = metaInfo;
        OutputDirectory = Path.Combine(outputDirectory, MetaInfo.Title ?? "");
        _peersListener = peersListener;
        _myBitfield = new Bitfield(metaInfo.Info.Pieces.Length / 20);
        _pieceStorage = new DiskStorage(
            OutputDirectory,
            files,
            (int)metaInfo.Info.PieceLength,
            metaInfo.Info.PiecesHashes
        );
        _piecePicker = new PiecePicker(
            _myBitfield,
            16 * 1024, //TODO This should be constant?
            (int)metaInfo.Info.PieceLength,
            totalSize
        );

        var dataStatistics = new DataStatistics(totalSize);
        var checkStatistics = new CheckStatistics(_myBitfield.Length);

        _myBitfield.SetPieces(downloadedPieces);
        checkStatistics.SetCheckedPieces(_myBitfield.Length);
        dataStatistics.SetVerifiedBytes(_piecePicker.GetBitfieldSize());

        var activePeers = new ConcurrentDictionary<PeerEndpoint, IPeerConnection>();
        _requestScheduler = new RequestScheduler(
            activePeers,
            _piecePicker,
            _myBitfield,
            dataStatistics,
            torrentClientOptions.WarmupTime,
            30.Seconds,
            _pieceStorage,
            torrentClientOptions.Logger
        );
        _uploadScheduler = new UploadScheduler(
            activePeers,
            _pieceStorage,
            _myBitfield,
            dataStatistics,
            torrentClientOptions.Logger
        );

        _peersClient = new PeersClient(
            activePeers,
            peerId,
            _requestScheduler,
            _uploadScheduler,
            _piecePicker,
            _myBitfield,
            torrentClientOptions.Logger
        );
        _trackerClient = new TrackerClient(
            _myBitfield,
            trackerHandlers,
            torrentClientOptions.UsedTrackers,
            peersListener.Port,
            dataStatistics,
            peerId,
            trackersChannel.Writer,
            metaInfo.AnnounceList?.Select(i => i.ToArray()).ToList() //Don't modify the original announce list
                ??
                [
                    [metaInfo.Announce],
                ],
            metaInfo.Info.InfoHash,
            torrentClientOptions.Logger
        );
        _peerConnector = new TcpPeersConnector(
            _peersClient,
            metaInfo.Info.InfoHash,
            peerId,
            trackersChannel,
            torrentClientOptions.PeerIpProxy,
            torrentClientOptions.Logger
        );

        if (torrentClientOptions.DhtOptions.Enabled)
        {
            var selfNodeId = NodeId.FromPeerId(peerId);
            var udpClient = new UdpClientWrapper(
                UdpClient.GetFreeUdpClient(
                    torrentClientOptions.ListenIpv4Address ?? IPAddress.Any,
                    torrentClientOptions.DhtOptions.Port
                )
            );
            var dhtHandler = new DhtHandler(
                udpClient,
                torrentClientOptions.Logger,
                retryDelay: 2.Seconds,
                retryLoopDelay: 250.Milliseconds,
                maxRetries: 2
            );
            _dhtClient = new DhtClient(
                selfNodeId,
                metaInfo.Info.InfoHash,
                dhtHandler,
                trackersChannel.Writer,
                torrentClientOptions.DhtOptions,
                peersListener.Port,
                torrentClientOptions.Logger
            );
        }

        Completion = new CompletionTracker(_myBitfield);
        Statistics = new TorrentStatisticsClient(
            dataStatistics,
            new PeerStatistics(_peersClient),
            checkStatistics
        );
    }

    /// <summary>
    /// Asynchronously stops the torrent operation and waits for any ongoing tasks to complete.
    /// </summary>
    /// <returns>A task that represents the asynchronous stop operation. The task completes when all related operations have
    /// finished. This will make Statistics.Completion be canceled if it wasn't completed</returns>
    public async ValueTask StopAsync()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await StopAndWaitToFinishAsync().ConfigureAwait(false);
        State = State.Stopped;
    }

    /// <summary>
    /// Starts the operation if it is not already running.
    /// </summary>
    /// <remarks>If the operation has already been started, this method has no effect. Throws an exception if
    /// the object has been disposed.</remarks>
    public async ValueTask StartAsync()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (State == State.Started)
        {
            return;
        }
        await StopAndWaitToFinishAsync().ConfigureAwait(false);
        Completion.Reset();
        _cancellationTokenSource?.Dispose();
        _cancellationTokenSource = new CancellationTokenSource();
        State = State.Started;
        _peersListener.AddPeersClient(MetaInfo.Info.InfoHash, _peersClient);
        _runTask = StartAndWaitToFinishAsync(_cancellationTokenSource);
    }

    /// <summary>
    /// Requests the torrent to be stopped.
    /// </summary>
    /// <remarks>This will make Statistics.Completion be canceled if it wasn't completed</remarks>
    public void Stop()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _cancellationTokenSource?.Cancel();
        Completion.TrySetCanceled();
        _peersListener.RemovePeersClient(MetaInfo.Info.InfoHash);
    }

    private async Task StartAndWaitToFinishAsync(CancellationTokenSource cancellationTokenSource)
    {
        try
        {
            Func<CancellationToken, Task>[] tasks =
            [
                _peersClient.StartAsync,
                _trackerClient.StartAsync,
                _peerConnector.StartAsync,
                _requestScheduler.StartAsync,
                _uploadScheduler.StartAsync,
                .. (
                    _dhtClient is not null
                        ? (Func<CancellationToken, Task>[])[_dhtClient.StartAsync]
                        : []
                ),
            ];
            await Task.RunUntilFirstCompletesAsync(tasks, cancellationTokenSource)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            if (
                ex is OperationCanceledException oce
                && oce.CancellationToken == cancellationTokenSource.Token
            )
            {
                Completion.TrySetCanceled();
                State = State.Stopped;
                return;
            }

            Completion.TrySetException(ex);
        }
    }

    private async ValueTask StopAndWaitToFinishAsync()
    {
        _cancellationTokenSource?.Cancel();
        Completion.TrySetCanceled();
        _peersListener.RemovePeersClient(MetaInfo.Info.InfoHash);
        if (_runTask is not null)
        {
            try
            {
                await _runTask.ConfigureAwait(false);
            }
            catch { }
        }
    }

    /// <summary>
    /// Asynchronously exports the current metadata to a file at the specified path in encoded format.
    /// </summary>
    /// <remarks>If the specified directory in the output path does not exist, it is created before writing
    /// the file. The method overwrites the file if it already exists.</remarks>
    /// <param name="outputPath">The file path where the exported metadata will be saved. If the directory does not exist, it will be created.
    /// Cannot be null or empty.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the export operation.</param>
    /// <returns></returns>
    public async Task ExportAsync(string outputPath, CancellationToken cancellationToken = default)
    {
        var folder = Path.GetDirectoryName(outputPath);

        if (folder is not null)
        {
            Directory.CreateDirectory(folder);
        }

        var rawMetainfo = MetaInfo.ToBDictionary();
        await using var encoder = new BEncoder(File.Open(outputPath, FileMode.OpenOrCreate));
        await encoder.EncodeAsync(rawMetainfo, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously read files in the output folder and verify it's content
    /// </summary>
    /// <param name="outputPath"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async ValueTask CheckAsync(CancellationToken cancellationToken = default)
    {
        State = State.Checking;
        await StopAndWaitToFinishAsync().ConfigureAwait(false);
        _myBitfield.Reset();
        Statistics.Check.Reset();

        await foreach (
            var (pieceIndex, isValid) in _pieceStorage.CheckPiecesAsync(cancellationToken)
        )
        {
            if (isValid)
                _myBitfield.SetPiece(pieceIndex);
            Statistics.Check.AddCheckedPiece();
        }

        Statistics.Data.SetVerifiedBytes(_piecePicker.GetBitfieldSize());
        State = State.Stopped;
    }

    /// <summary>
    /// Returns swarm statistics from the first responsive tracker, or null if no tracker supports scrape.
    /// </summary>
    public ValueTask<ScrapeInfo?> ScrapeAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _trackerClient.ScrapeAsync(cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            await StopAndWaitToFinishAsync().ConfigureAwait(false);
            await _peersClient.DisposeAsync().ConfigureAwait(false);
            if (_dhtClient is not null)
                await _dhtClient.DisposeAsync().ConfigureAwait(false);
            await _trackerClient.DisposeAsync().ConfigureAwait(false);
            await _uploadScheduler.DisposeAsync().ConfigureAwait(false);
            await _requestScheduler.DisposeAsync().ConfigureAwait(false);
            _pieceStorage.Dispose();
            Completion.Dispose();
            _cancellationTokenSource?.Dispose();
            _cancellationTokenSource = null;
            _runTask = null;
            State = State.Disposed;
        }
    }
}
