using Netorrent.IO;

namespace Netorrent.Tests.Fakes;

internal class FileHandlerFake(ulong Left, ulong Downloaded, ulong Uploaded) : IFilesHandler
{
    public ulong GetDownloaded() => Downloaded;

    public ulong GetLeft() => Left;

    public ulong GetUploaded() => Uploaded;
}
