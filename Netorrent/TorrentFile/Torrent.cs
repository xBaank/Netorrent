using Netorrent.Bencoding;
using Netorrent.Bencoding.Structs;
using Netorrent.Extensions;
using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.TorrentFile;

public class Torrent
{
    public MetaInfo MetaInfo { get; init; }

    public static async ValueTask<Torrent> Create(
        string path,
        CancellationToken cancellationToken = default
    )
    {
        var torrentFileData = File.ReadAllBytesAsync(path, cancellationToken);
        var torrent = new Torrent(await torrentFileData);
        return torrent;
    }

    public Torrent(ReadOnlySpan<byte> data)
    {
        var decoder = new BDecoder(data);
        var decoded = decoder.Decode();
        if (decoded is not BDictionary bDictionary)
            throw new InvalidDataException("Torrent file is not a valid bencoded dictionary.");

        MetaInfo = ParseMetaInfo(bDictionary);
    }

    private static MetaInfo ParseMetaInfo(BDictionary dictionary)
    {
        var info =
            dictionary.Elements["info"].As<BDictionary>() ?? throw new InvalidDataException();
        var announce =
            dictionary.Elements["announce"].As<BString>() ?? throw new InvalidDataException();
        var announceList = dictionary
            .Elements.GetValueOrDefault("announce-list")
            ?.As<BList>()
            ?.Elements?.Select(i => i.As<BList>()!.Value.Elements)
            .SelectMany(i => i)
            .Select<IBencodingType, string>(i => i.As<BString>()!.Value)
            .ToList();
        var creationDate = dictionary.Elements.GetValueOrDefault("creation date")?.As<BInt>();
        var comment = dictionary.Elements.GetValueOrDefault("comment")?.As<BString>();
        var createdBy = dictionary.Elements.GetValueOrDefault("created by")?.As<BString>();
        var encoding = dictionary.Elements.GetValueOrDefault("encoding")?.As<BString>();

        //info parts
        var name = info.Elements["name"].As<BString>() ?? throw new InvalidDataException();
        var pieceLength =
            info.Elements["piece length"].As<BInt>() ?? throw new InvalidDataException();
        var pieces = info.Elements["pieces"].As<BString>() ?? throw new InvalidDataException();
        var privateFlag = info.Elements.GetValueOrDefault("private").As<BInt>();

        //Single mode
        var length = info.Elements.GetValueOrDefault("length").As<BInt>();
        var md5sum = info.Elements.GetValueOrDefault("md5sum").As<BString>();

        //Multi mode
        var files = info
            .Elements.GetValueOrDefault("files")
            .As<BList>()
            ?.Elements?.Select(ParseFile)
            .ToList();

        return new MetaInfo(
            Info: new Info(
                pieceLength,
                pieces,
                privateFlag ?? 0,
                files is null ? InfoType.Single : InfoType.Multiple,
                name,
                length,
                md5sum,
                files
            ),
            Announce: announce,
            AnnounceList: announceList,
            CreationDate: creationDate,
            Comment: comment,
            CreatedBy: createdBy,
            Encoding: encoding
        );
    }

    public static InfoFile ParseFile(IBencodingType data)
    {
        var dic = data.As<BDictionary>() ?? throw new InvalidDataException();
        var length = dic.Elements["length"].As<BInt>() ?? throw new InvalidDataException();
        var path =
            dic.Elements["path"]
                .As<BList>()
                ?.Elements?.Select<IBencodingType, string>(i =>
                    i.As<BString>() ?? throw new InvalidDataException()
                )
                ?.ToList()
            ?? throw new InvalidDataException();
        var md5sum = dic.Elements.GetValueOrDefault("md5sum")?.As<BString>();

        return new InfoFile(length, path, md5sum);
    }
}
