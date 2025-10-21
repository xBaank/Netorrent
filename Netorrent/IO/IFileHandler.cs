using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.IO;

internal interface IFilesHandler
{
    ulong GetDownloaded();
    ulong GetUploaded();
    ulong GetLeft();
}

internal class FilesHandler(MetaInfo metaInfo) : IFilesHandler
{
    public ulong GetDownloaded()
    {
        //TODO Implement logic to get downloaded size
        return 0;
    }

    public ulong GetUploaded()
    {
        //TODO Implement logic to get uploaded size
        return 0;
    }

    public ulong GetLeft() =>
        (ulong)(
            metaInfo.Info.Type == InfoType.Single
                ? metaInfo.Info.Length ?? 0
                : metaInfo.Info.Files?.Sum(f => f.Length) ?? 0
        );
}
