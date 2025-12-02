using System.Net;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Netorrent.Bencoding;
using Netorrent.IO;
using Netorrent.P2P;
using Netorrent.P2P.Messages;
using Netorrent.TorrentFile.FileStructure;
using Netorrent.Tracker;
using Netorrent.Tracker.Udp;

namespace Netorrent.TorrentFile;

public sealed class Torrent : IAsyncDisposable
{
    public MetaInfo MetaInfo { get; init; }
    public Bitfield Bitfield => _myBitfield;
    public DownloadInfo DownloadInfo => _p2pClient.DownloadInfo;
    public string OutputDirectory => _fileManager.OutputDirectory;

    public State State { get; private set; } = State.Stopped;
    public Task? TorrentTask { get; private set; }

    private readonly P2PClient _p2pClient;
    private readonly TrackerClient _trackerClient;
    private readonly FileManager _fileManager;
    private readonly Bitfield _myBitfield;
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
            _p2pClient,
            peerId,
            trackersChannel.Writer,
            metaInfo,
            logger,
            forcedIp
        );
    }

    private async Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            List<Task> tasks =
            [
                _p2pClient.StartAsync(cancellationToken),
                _trackerClient.StartAsync(cancellationToken),
            ];
            var finishedTask = await Task.WhenAny(tasks);
            _cancellationTokenSource?.Cancel();
            await Task.WhenAll(tasks);
            await finishedTask;
        }
        catch (OperationCanceledException)
        {
            DownloadInfo.SetCanceled();
        }
        catch (Exception ex)
        {
            DownloadInfo.SetException(ex);
        }
    }

    /// <summary>
    /// Starts the download process if it is not already running.
    /// </summary>
    /// <remarks>If the download is already started, this method has no effect. Once started, the download
    /// process can be canceled using the appropriate cancellation mechanism.</remarks>
    public async ValueTask StartAsync()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (State == State.Started)
            return;

        _cancellationTokenSource?.Cancel();
        await (TorrentTask ?? Task.CompletedTask);

        DownloadInfo.Reset();
        _cancellationTokenSource?.Dispose();
        _cancellationTokenSource = new();
        _cancellationTokenSource.Token.Register(_p2pClient.DownloadInfo.SetCanceled);
        State = State.Started;
        TorrentTask = StartAsync(_cancellationTokenSource.Token);
    }

    /// <summary>
    /// Asynchronously stops the torrent operation and waits for any ongoing tasks to complete.
    /// </summary>
    /// <returns>A task that represents the asynchronous stop operation. The task completes when all related operations have
    /// finished.</returns>
    public async ValueTask StopAsync()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        Stop();
        await (TorrentTask ?? Task.CompletedTask);
    }

    private void Stop()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _cancellationTokenSource?.Cancel();
        State = State.Stopped;
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
        await File.WriteAllBytesAsync(outputPath, encoder.Encode(rawMetainfo), cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _cancellationTokenSource?.Cancel();
            await (TorrentTask ?? Task.CompletedTask);
            _fileManager.Dispose();
            await _p2pClient.DisposeAsync();
            await _trackerClient.DisposeAsync();
            _cancellationTokenSource?.Dispose();
            _cancellationTokenSource = null;
            TorrentTask = null;
            State = State.Disposed;
            _disposed = true;
        }
    }
}
