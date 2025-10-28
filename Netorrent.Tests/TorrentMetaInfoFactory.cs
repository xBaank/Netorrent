using System.Security.Cryptography;
using System.Text;
using Netorrent.Bencoding.Structs;
using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.Tests;

public static class TestMetaInfoFactory
{
    /// <summary>
    /// Creates a simple single-file MetaInfo for testing
    /// </summary>
    public static MetaInfo CreateSingleFileMetaInfo(
        string announceUrl,
        string fileName,
        string fileContent
    )
    {
        var fileBytes = Encoding.Latin1.GetBytes(fileContent);

        const int pieceLength = 16 * 1024; // 16 KB

        // Compute pieces
        var piecesBytes = new List<byte>();
        for (int offset = 0; offset < fileBytes.Length; offset += pieceLength)
        {
            int len = Math.Min(pieceLength, fileBytes.Length - offset);
            var hash = SHA1.HashData(fileBytes.AsSpan(offset, len));
            piecesBytes.AddRange(hash);
        }

        // Build the info dictionary
        var infoDict = new Dictionary<BString, IBencodingNode>
        {
            [new BString("name")] = new BString(fileName),
            [new BString("length")] = new BInt(fileBytes.Length),
            [new BString("piece length")] = new BInt(pieceLength),
            [new BString("pieces")] = new BString(Encoding.ASCII.GetString(piecesBytes.ToArray())),
        };

        var rawInfo = new BDictionary(infoDict);

        var info = new Info(
            RawInfo: rawInfo,
            PieceLength: pieceLength,
            Pieces: piecesBytes.ToArray(),
            Private: 0,
            Type: InfoType.Single,
            Name: fileName,
            Length: fileBytes.Length
        );

        return new MetaInfo(info, announceUrl);
    }

    /// <summary>
    /// Creates a simple multi-file MetaInfo for testing
    /// </summary>
    public static MetaInfo CreateMultiFileMetaInfo(
        string announceUrl,
        Dictionary<string, string> files
    )
    {
        const int pieceLength = 16 * 1024;

        // Flatten all file contents to calculate pieces
        var allBytes = new List<byte>();
        foreach (var content in files.Values)
        {
            allBytes.AddRange(Encoding.Latin1.GetBytes(content));
        }

        var piecesBytes = new List<byte>();
        for (int offset = 0; offset < allBytes.Count; offset += pieceLength)
        {
            int len = Math.Min(pieceLength, allBytes.Count - offset);
            var hash = SHA1.HashData(allBytes.GetRange(offset, len).ToArray());
            piecesBytes.AddRange(hash);
        }

        // Build file list
        var infoFiles = new List<InfoFile>();
        foreach (var kv in files)
        {
            infoFiles.Add(new InfoFile(kv.Value.Length, [kv.Key]));
        }

        var infoDict = new Dictionary<BString, IBencodingNode>
        {
            [new BString("name")] = new BString("test-folder"),
            [new BString("piece length")] = new BInt(pieceLength),
            [new BString("pieces")] = new BString(Encoding.ASCII.GetString(piecesBytes.ToArray())),
            [new BString("files")] = new BList(
                [
                    .. infoFiles
                        .ConvertAll(f => new BDictionary(
                            new Dictionary<BString, IBencodingNode>
                            {
                                [new BString("length")] = new BInt(f.Length),
                                [new BString("path")] = new BList(
                                    f.Path.ConvertAll(p => new BString(p))
                                        .Cast<IBencodingNode>()
                                        .ToList()
                                ),
                            }
                        ))
                        .Cast<IBencodingNode>(),
                ]
            ),
        };

        var rawInfo = new BDictionary(infoDict);

        var info = new Info(
            RawInfo: rawInfo,
            PieceLength: pieceLength,
            Pieces: piecesBytes.ToArray(),
            Private: 0,
            Type: InfoType.Multiple,
            Name: "test-folder",
            Files: infoFiles
        );

        return new MetaInfo(info, announceUrl);
    }
}
