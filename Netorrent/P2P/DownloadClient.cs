using System.Net;
using Netorrent.IO;
using Netorrent.P2P.Measurement;
using Netorrent.P2P.Messages;

namespace Netorrent.P2P;

public class DownloadInfo
{
    private readonly IReadOnlyDictionary<IPEndPoint, PeerConnection> _peers;
    private readonly FileManager _fileManager;
    private readonly Bitfield _bitfield;
    private TaskCompletionSource _downloadTaskCompletitionSource = new();

    private IEnumerable<PeerConnection> PeersNotChocking =>
        _peers.Values.Where(i => !i.PeerChocking && i.AmInterested);
    public int ActivePeers => PeersNotChocking.Count();
    public int TotalPeers => _peers.Values.Count();
    public DownloadSpeed DownloadSpeed => PeersNotChocking.Sum(p => p.SpeedTracker.CurrentBps.Bps);
    public ByteSize DownloadedBytes => PeersNotChocking.Sum(p => p.SpeedTracker.TotalBytes.Bytes);
    public ByteSize TotalBytes => _fileManager.TotalSize;
    public Task DownloadTask => _downloadTaskCompletitionSource.Task;

    internal DownloadInfo(
        IReadOnlyDictionary<IPEndPoint, PeerConnection> peers,
        FileManager fileManager,
        Bitfield bitfield
    )
    {
        _peers = peers;
        _fileManager = fileManager;
        _bitfield = bitfield;
        _bitfield.OnHavePieceAsync += CheckDownload;
    }

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

    private Task CheckDownload(int pieceIndex, CancellationToken token)
    {
        if (_bitfield.IsComplete)
        {
            _downloadTaskCompletitionSource.TrySetResult();
        }

        return Task.CompletedTask;
    }

    internal void Dispose()
    {
        _bitfield.OnHavePieceAsync -= CheckDownload;
    }
}
