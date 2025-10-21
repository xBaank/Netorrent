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
    string Pieces,
    long Private,
    InfoType Type,
    string Name,
    //Single file mode
    long? Length = null,
    string? Md5sum = null,
    //Multiple file mode
    List<InfoFile>? Files = null
)
{
    public readonly byte[] InfoHash = ComputeInfoHash(RawInfo);

    private static byte[] ComputeInfoHash(BDictionary info)
    {
        var encoder = new BEncoder();

        var infoBytes = encoder.Encode(info);
        return SHA1.HashData(infoBytes);
    }

    public ulong GetAllFilesSize() =>
        (ulong)(Type == InfoType.Single ? Length ?? 0 : Files?.Sum(f => f.Length) ?? 0);
}

public record InfoFile(long Length, List<string> Path, string? Md5sum = null);
