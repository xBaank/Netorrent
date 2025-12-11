using Netorrent.IO;
using Netorrent.P2P;
using Netorrent.P2P.Measurement;
using Netorrent.P2P.Messages;
using ZLinq;
using ZLinq.Linq;

namespace Netorrent.Stats;

public class StatsClient
{
    private readonly IReadOnlyDictionary<PeerEndpoint, PeerConnection> _peers;
    private readonly Bitfield _bitfield;
    private readonly IDisposable _stateDisposable;
    private TaskCompletionSource _downloadTaskCompletitionSource = new();
    private long _downloadedBytes;
    private long _uploadedBytes;
    private readonly long _totalBytes;

    private ValueEnumerable<
        Where<FromEnumerable<PeerConnection>, PeerConnection>,
        PeerConnection
    > PeersNotChocking =>
        _peers.Values.AsValueEnumerable().Where(i => !i.PeerChoking && i.AmInterested);

    public int ActivePeers => PeersNotChocking.Count();
    public int TotalPeers => _peers.Values.AsValueEnumerable().Count();
    public DownloadSpeed DownloadSpeed =>
        PeersNotChocking.Sum(p => p.DownloadSpeedTracker.CurrentBps.Bps);
    public ByteSize DownloadedBytes => _downloadedBytes;
    public ByteSize UploadedBytes => _uploadedBytes;
    public ByteSize TotalBytes => _totalBytes;
    public ByteSize LeftBytes => TotalBytes - DownloadedBytes;

    public Task DownloadTask => _downloadTaskCompletitionSource.Task;

    internal StatsClient(
        IReadOnlyDictionary<PeerEndpoint, PeerConnection> peers,
        long totalSize,
        Bitfield bitfield
    )
    {
        _peers = peers;
        _totalBytes = totalSize;
        _bitfield = bitfield;
        _stateDisposable = _bitfield.StateChanged.Subscribe(CheckDownload);
    }

    internal void AddDownloadedBytes(long bytes) => Interlocked.Add(ref _downloadedBytes, bytes);

    internal void AddUploadedBytes(long bytes) => Interlocked.Add(ref _uploadedBytes, bytes);

    internal void Reset()
    {
        _downloadTaskCompletitionSource.TrySetCanceled();
        _downloadTaskCompletitionSource = new();
        if (_bitfield.IsComplete)
            _downloadTaskCompletitionSource.TrySetResult();
    }

    internal void SetException(Exception exception)
    {
        _downloadTaskCompletitionSource.TrySetException(exception);
    }

    internal void SetCanceled()
    {
        _downloadTaskCompletitionSource.TrySetCanceled();
    }

    private void CheckDownload(int pieceIndex)
    {
        if (_bitfield.IsComplete)
        {
            _downloadTaskCompletitionSource.TrySetResult();
        }
    }

    internal void Dispose()
    {
        _stateDisposable.Dispose();
    }
}
