using System.Net;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Bencoding;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P;
using Netorrent.P2P.Messages;
using Netorrent.Statistics;
using Netorrent.TorrentFile.FileStructure;
using Netorrent.Tracker;
using Netorrent.Tracker.Udp;

namespace Netorrent.TorrentFile;

public sealed class Torrent : IAsyncDisposable
{
    public MetaInfo MetaInfo { get; init; }
    public TorrentStatisticsClient Statistics => _p2pClient.Stats;
    public string OutputDirectory => _fileManager.OutputDirectory;

    public State State { get; private set; } = State.Stopped;

    private readonly P2PClient _p2pClient;
    private readonly TrackerClient _trackerClient;
    private readonly FileManager _fileManager;
    private readonly Bitfield _myBitfield;
    private Task? _runTask;
    private CancellationTokenSource? _cancellationTokenSource;
    private bool _disposed;

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
            _p2pClient.EndPoint.Port,
            _p2pClient.Stats.Transfer,
            peerId,
            trackersChannel.Writer,
            [metaInfo.Announce, .. metaInfo.AnnounceList ?? []],
            metaInfo.Info.InfoHash,
            logger,
            forcedIp
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
            return;

        await StopAndWaitToFinishAsync();
        Statistics.Completion.Reset();
        _cancellationTokenSource?.Dispose();
        _cancellationTokenSource = new CancellationTokenSource();
        State = State.Started;
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
        Statistics.Completion.TrySetCanceled();
    }

    private async Task StartAndWaitToFinishAsync(CancellationTokenSource cancellationTokenSource)
    {
        try
        {
            await cancellationTokenSource
                .CancelOnFirstCompletionAndAwaitAllAsync([
                    _p2pClient.StartAsync(cancellationTokenSource.Token),
                    _trackerClient.StartAsync(cancellationTokenSource.Token),
                ])
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Statistics.Completion.TrySetException(ex);
            return;
        }

        //If no exception was thrown or the token is cancelled then we set it to canceled
        if (cancellationTokenSource.Token.IsCancellationRequested)
        {
            Statistics.Completion.TrySetCanceled();
            State = State.Stopped;
        }
    }

    private async ValueTask StopAndWaitToFinishAsync()
    {
        _cancellationTokenSource?.Cancel();
        Statistics.Completion.TrySetCanceled();
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
            Directory.CreateDirectory(folder);

        var rawMetainfo = MetaInfo.ToBDictionary();
        await using var encoder = new BEncoder();
        await File.WriteAllBytesAsync(outputPath, encoder.Encode(rawMetainfo), cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            await StopAndWaitToFinishAsync().ConfigureAwait(false);
            _fileManager.Dispose();
            await _p2pClient.DisposeAsync().ConfigureAwait(false);
            await _trackerClient.DisposeAsync().ConfigureAwait(false);
            _cancellationTokenSource?.Dispose();
            _cancellationTokenSource = null;
            _runTask = null;
            State = State.Disposed;
        }
    }
}
