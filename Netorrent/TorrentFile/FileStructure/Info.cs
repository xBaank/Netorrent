using Netorrent.Bencoding.Structs;
using ZLinq;

namespace Netorrent.TorrentFile.FileStructure;

public enum InfoType
{
    Single,
    Multiple,
}

public record Info(
    InfoHash InfoHash,
    BDictionary RawInfo,
    long PieceLength,
    byte[] Pieces,
    long? Private,
    InfoType Type,
    string Name,
    //Single file mode
    long? Length = null,
    string? Md5sum = null,
    //Multiple file mode
    List<InfoFile>? Files = null
)
{
    public IReadOnlyList<InfoFile> NormalizedFiles { get; } =
        Type == InfoType.Single ? [new InfoFile(Length ?? 0, [Name], Md5sum)] : Files ?? [];
    public IReadOnlyList<byte[]> PiecesHashes { get; } =
        Pieces.AsValueEnumerable().Chunk(20).ToList();
}

public record InfoFile(long Length, List<string> Path, string? Md5sum = null);
