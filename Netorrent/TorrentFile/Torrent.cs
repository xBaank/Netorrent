using System.Buffers;
using System.Collections.Concurrent;
using System.Net;
using System.Threading.Channels;
using Netorrent.Bencoding;
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
using Netorrent.Tracker;
using Netorrent.Tracker.Http;
using Netorrent.Tracker.Udp;
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
    private readonly TcpPeersListener _peersListener;
    private readonly PeersClient _peersClient;
    private readonly TrackerClient _trackerClient;
    private readonly DiskStorage _pieceStorage;
    private readonly Bitfield _myBitfield;
    private readonly IPiecePicker _piecePicker;
    private Task? _runTask;
    private CancellationTokenSource? _cancellationTokenSource;
    private bool _disposed;

    internal Torrent(
        MetaInfo metaInfo,
        IHttpTrackerHandler httpTrackerHandler,
        IUdpTrackerHandler udpTrackerHandler,
        TcpPeersListener peersListener,
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
        var requestScheduler = new RequestScheduler(
            activePeers,
            _piecePicker,
            _myBitfield,
            dataStatistics,
            torrentClientOptions.WarmupTime,
            _pieceStorage,
            torrentClientOptions.Logger
        );
        var uploadScheduler = new UploadScheduler(
            activePeers,
            _pieceStorage,
            _myBitfield,
            dataStatistics,
            torrentClientOptions.Logger
        );

        _peersClient = new PeersClient(
            activePeers,
            peerId,
            requestScheduler,
            uploadScheduler,
            _piecePicker,
            _myBitfield,
            torrentClientOptions.Logger
        );
        _trackerClient = new TrackerClient(
            httpTrackerHandler,
            udpTrackerHandler,
            torrentClientOptions.SupportedAddressFamilies,
            torrentClientOptions.UsedTrackers,
            peersListener.EndPoint.Port,
            dataStatistics,
            peerId,
            trackersChannel.Writer,
            [metaInfo.Announce, .. metaInfo.AnnounceList ?? []],
            metaInfo.Info.InfoHash,
            torrentClientOptions.Logger,
            torrentClientOptions.ForcedIp
        );
        _peerConnector = new TcpPeersConnector(
            _peersClient,
            metaInfo.Info.InfoHash,
            torrentClientOptions.SupportedAddressFamilies,
            peerId,
            trackersChannel,
            torrentClientOptions.PeerIpProxy,
            torrentClientOptions.Logger
        );

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
            await cancellationTokenSource
                .CancelOnFirstCompletionAndAwaitAllAsync([
                    _peersClient.StartAsync(cancellationTokenSource.Token),
                    _trackerClient.StartAsync(cancellationTokenSource.Token),
                    _peerConnector.StartAsync(cancellationTokenSource.Token),
                ])
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
        await using var encoder = new BEncoder();
        await File.WriteAllBytesAsync(outputPath, encoder.Encode(rawMetainfo), cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously read files in the output folder and verify it's content
    /// </summary>
    /// <param name="outputPath"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async ValueTask CheckAsync(CancellationToken cancellationToken = default)
    {
        State = State.Verifying;
        await StopAndWaitToFinishAsync().ConfigureAwait(false);
        _myBitfield.Reset();
        Statistics.Check.Reset();
        var channelSize = 64;
        var piecesChannel = Channel.CreateBounded<(int PieceIndex, RentedArray<byte> Piece)>(
            new BoundedChannelOptions(channelSize) { SingleReader = true, SingleWriter = true }
        );
        var processPiecesTask = ProcessPiecesAsync();
        var getPiecesTask = GetPiecesAsync();

        await Task.WhenAll(processPiecesTask, getPiecesTask).ConfigureAwait(false);

        Statistics.Data.SetVerifiedBytes(_piecePicker.GetBitfieldSize());

        async Task ProcessPiecesAsync()
        {
            await foreach (
                var items in piecesChannel.Reader.ReadAllAsync(cancellationToken).Chunk(16)
            )
            {
                Parallel.ForEach(
                    items,
                    (item) => VerifyPiece(item.PieceIndex, item.Piece, cancellationToken)
                );
            }
        }

        async Task GetPiecesAsync()
        {
            var pieceIndex = 0;
            var bufferPos = 0;
            var pieceLength = (int)MetaInfo.Info.PieceLength;
            using var pool = MemoryPool<byte>.Shared.Rent(pieceLength);
            var pieceBuffer = pool.Memory[..pieceLength];

            foreach (var filePath in MetaInfo.Info.NormalizedFiles)
            {
                var fileRemaining = filePath.Length;
                var path = Path.Combine([OutputDirectory, .. filePath.Path]);
                cancellationToken.ThrowIfCancellationRequested();

                await using var fs = new FileStream(
                    path,
                    FileMode.OpenOrCreate,
                    FileAccess.Read,
                    FileShare.ReadWrite,
                    4096,
                    FileOptions.SequentialScan | FileOptions.Asynchronous
                );
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var toRead = pieceLength - bufferPos;
                    await fs.ReadAsync(pieceBuffer.Slice(bufferPos, toRead), cancellationToken)
                        .ConfigureAwait(false);
                    if (fileRemaining >= toRead)
                    {
                        fileRemaining -= toRead;
                        bufferPos += toRead;
                    }
                    else
                    {
                        bufferPos += (int)fileRemaining;
                        fileRemaining = 0;
                        break;
                    }

                    // If buffer is full, verify piece
                    if (bufferPos == pieceLength)
                    {
                        var rentedArray = new RentedArray<byte>(
                            ArrayPool<byte>.Shared.Rent(pieceBuffer.Length),
                            pieceBuffer.Length
                        );
                        pieceBuffer.CopyTo(rentedArray.Memory);
                        await piecesChannel
                            .Writer.WriteAsync((pieceIndex, rentedArray), cancellationToken)
                            .ConfigureAwait(false);
                        pieceBuffer.Span.Clear();
                        pieceIndex++;
                        bufferPos = 0;
                    }
                }
            }

            if (bufferPos > 0)
            {
                var lastPiece = pieceBuffer[..bufferPos];
                var rentedArray = new RentedArray<byte>(
                    ArrayPool<byte>.Shared.Rent(lastPiece.Length),
                    lastPiece.Length
                );
                lastPiece.CopyTo(rentedArray.Memory);
                await piecesChannel
                    .Writer.WriteAsync((pieceIndex, rentedArray), cancellationToken)
                    .ConfigureAwait(false);
                pieceIndex++;
            }

            piecesChannel.Writer.TryComplete();
        }

        void VerifyPiece(
            int pieceIndex,
            RentedArray<byte> pieceData,
            CancellationToken cancellationToken
        )
        {
            using (pieceData)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var hasPiece = _pieceStorage.VerifyPiece(pieceIndex, pieceData.Memory);

                if (hasPiece)
                {
                    _myBitfield.SetPiece(pieceIndex);
                }

                Statistics.Check.AddCheckedPiece();
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            await StopAndWaitToFinishAsync().ConfigureAwait(false);
            _pieceStorage.Dispose();
            await _peersClient.DisposeAsync().ConfigureAwait(false);
            await _trackerClient.DisposeAsync().ConfigureAwait(false);
            Completion.Dispose();
            _cancellationTokenSource?.Dispose();
            _cancellationTokenSource = null;
            _runTask = null;
            State = State.Disposed;
        }
    }
}
