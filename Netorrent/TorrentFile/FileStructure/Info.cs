namespace Netorrent.TorrentFile.FileStructure;

public enum InfoType
{
    Single,
    Multiple,
}

public record Info(
    long PieceLength,
    string Pieces,
    long Private,
    InfoType Type,
    string Name,
    //Single file mode
    long? Length = null,
    string? Md5sum = null,
    //Multiple file mode
    List<InfoFile>? Files = null
);

public record InfoFile(long Length, List<string> Path, string? Md5sum = null);
