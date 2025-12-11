using Netorrent.P2P.Measurement;

namespace Netorrent.Statistics;

public class TransferStatistics(long totalBytes)
{
    private long _downloadedBytes;
    private long _uploadedBytes;

    public ByteSize DownloadedBytes => Interlocked.Read(ref _downloadedBytes);
    public ByteSize UploadedBytes => Interlocked.Read(ref _uploadedBytes);
    public ByteSize TotalBytes => totalBytes;
    public ByteSize LeftBytes => TotalBytes - DownloadedBytes;

    internal void AddDownloadedBytes(long bytes) => Interlocked.Add(ref _downloadedBytes, bytes);

    internal void AddUploadedBytes(long bytes) => Interlocked.Add(ref _uploadedBytes, bytes);
}
