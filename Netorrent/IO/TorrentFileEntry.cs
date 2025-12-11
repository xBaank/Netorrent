using Microsoft.Win32.SafeHandles;

namespace Netorrent.IO;

internal sealed record TorrentFileEntry(
    string FullPath,
    long StartOffset,
    long Length,
    SafeFileHandle SafeHandle
)
{
    public long EndOffset => StartOffset + Length;
    public bool IsDirectoryCreated { get; set; } = false;
}
