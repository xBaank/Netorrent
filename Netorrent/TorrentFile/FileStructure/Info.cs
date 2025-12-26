using System.Security.Cryptography;
using Netorrent.Bencoding;
using Netorrent.Bencoding.Structs;

namespace Netorrent.TorrentFile.FileStructure;

public enum InfoType
{
    Single,
    Multiple,
}

public record Info(
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
    public InfoHash InfoHash = ComputeInfoHash(RawInfo);

    private static InfoHash ComputeInfoHash(BDictionary info)
    {
        using var encoder = new BEncoder();
        var infoBytes = encoder.Encode(info);
        return SHA1.HashData(infoBytes);
    }

    public List<InfoFile> NormalizedFiles() =>
        Type == InfoType.Single ? [new InfoFile(Length ?? 0, [Name], Md5sum)] : Files ?? [];
}

public record InfoFile(long Length, List<string> Path, string? Md5sum = null);
