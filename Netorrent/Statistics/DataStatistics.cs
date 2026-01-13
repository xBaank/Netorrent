using Netorrent.P2P.Measurement;

namespace Netorrent.Statistics;

public class DataStatistics(long totalBytes)
{
    private long _verifiedBytes;
    private long _uploadedBytes;
    private long _discardedBytes;
    private long _downloadedBytes;

    public ByteSize Verified => Interlocked.Read(ref _verifiedBytes);
    public ByteSize Discarded => Interlocked.Read(ref _discardedBytes);
    public ByteSize Uploaded => Interlocked.Read(ref _uploadedBytes);
    public ByteSize Downloaded => Interlocked.Read(ref _downloadedBytes);
    public ByteSize Total => totalBytes;
    public ByteSize Left => Total - Downloaded;

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
