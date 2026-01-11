using Netorrent.P2P.Measurement;

namespace Netorrent.Statistics;

public class TransferStatistics(long totalBytes)
{
    private long _verifiedBytes;
    private long _uploadedBytes;
    private long _discardedBytes;
    private long _downloadedBytes;

    public ByteSize VerifiedBytes => Interlocked.Read(ref _verifiedBytes);
    public ByteSize UploadedBytes => Interlocked.Read(ref _uploadedBytes);
    public ByteSize DiscardedBytes => Interlocked.Read(ref _discardedBytes);
    public ByteSize DownloadedBytes => Interlocked.Read(ref _downloadedBytes);
    public ByteSize TotalBytes => totalBytes;
    public ByteSize LeftBytes => TotalBytes - DownloadedBytes;

    internal void SetVerifiedBytes(long bytes)
    {
        Interlocked.Exchange(ref _verifiedBytes, bytes);
        Interlocked.Exchange(ref _downloadedBytes, bytes);
    }

    internal void AddVerifiedBytes(long bytes)
    {
        Interlocked.Add(ref _verifiedBytes, bytes);
        Interlocked.Add(ref _downloadedBytes, bytes);
    }

    internal void AddDiscardedBytes(long bytes)
    {
        Interlocked.Add(ref _discardedBytes, bytes);
        Interlocked.Add(ref _downloadedBytes, -bytes);
    }

    internal void AddUploadedBytes(long bytes) => Interlocked.Add(ref _uploadedBytes, bytes);
}
