using Netorrent.P2P.Measurement;

namespace Netorrent.Statistics;

public class TransferStatistics(long totalBytes)
{
    private long _downloadedBytes;
    private long _uploadedBytes;
    private long _discardedBytes;
    private long _completedBytes;

    public ByteSize DownloadedBytes => Interlocked.Read(ref _downloadedBytes);
    public ByteSize UploadedBytes => Interlocked.Read(ref _uploadedBytes);
    public ByteSize DiscardedBytes => Interlocked.Read(ref _discardedBytes);
    public ByteSize CompletedBytes => Interlocked.Read(ref _completedBytes);
    public ByteSize TotalBytes => totalBytes;
    public ByteSize LeftBytes => TotalBytes - CompletedBytes;

    internal void AddDownloadedBytes(long bytes)
    {
        Interlocked.Add(ref _downloadedBytes, bytes);
        Interlocked.Add(ref _completedBytes, bytes);
    }

    internal void AddDiscardedBytes(long bytes)
    {
        Interlocked.Add(ref _discardedBytes, bytes);
        Interlocked.Add(ref _completedBytes, -bytes);
    }

    internal void AddUploadedBytes(long bytes) => Interlocked.Add(ref _uploadedBytes, bytes);
}
