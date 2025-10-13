using System.Numerics;
using Netorrent.Bencoding;
using Netorrent.Bencoding.Structs;
using Netorrent.Torrent.FileStructure;

namespace Netorrent.Torrent;

public class Torrent
{
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
        if (decoded is not BDictionary)
            throw new InvalidDataException("Torrent file is not a valid bencoded dictionary.");
    }

    //TODO use extension methods to safe cast values
    private MetaInfo ParseDictionary(BDictionary dictionary)
    {
        var info = dictionary.Elements["info"];
        var announce = dictionary.Elements["announce"];
        var announceList = dictionary.Elements.GetValueOrDefault("announce-list");
        var creationDate = dictionary.Elements.GetValueOrDefault("creation date");
        var comment = dictionary.Elements.GetValueOrDefault("comment");
        var createdBy = dictionary.Elements.GetValueOrDefault("created by");
        var encoding = dictionary.Elements.GetValueOrDefault("encoding");

        if (info is not BDictionary infoDict)
            throw new InvalidDataException("The 'info' field is not a valid bencoded dictionary.");
        if (announce is not BString announceString)
            throw new InvalidDataException("The 'announce' field is not a valid bencoded string.");
        if (announceList is not BList announceListList)
            throw new InvalidDataException(
                "The 'announce-list' field is not a valid bencoded list."
            );
        if (creationDate is not null && creationDate is not BString creationDateString)
            throw new InvalidDataException(
                "The 'creation date' field is not a valid bencoded string."
            );
        if (comment is not null && comment is not BString commentString)
            throw new InvalidDataException("The 'comment' field is not a valid bencoded string.");
        if (createdBy is not null && createdBy is not BString createdByString)
            throw new InvalidDataException(
                "The 'created by' field is not a valid bencoded string."
            );
        if (encoding is not null && encoding is not BString encodingString)
            throw new InvalidDataException("The 'encoding' field is not a valid bencoded string.");

        var name = infoDict.Elements["name"];
        var pieceLength = infoDict.Elements["piece length"];
        var pieces = infoDict.Elements["pieces"];
        var privateFlag = infoDict.Elements.GetValueOrDefault("private");

        //Single mode
        var length = infoDict.Elements.GetValueOrDefault("length");
        var md5sum = infoDict.Elements.GetValueOrDefault("md5sum");

        //Multi mode
        var files = infoDict.Elements.GetValueOrDefault("files");

        if (name is not BString nameString)
            throw new InvalidDataException("The 'name' field is not a valid bencoded string.");
        if (pieceLength is not BInt pieceLengthInt)
            throw new InvalidDataException(
                "The 'piece length' field is not a valid bencoded integer."
            );
        if (pieces is not BString piecesString)
            throw new InvalidDataException("The 'pieces' field is not a valid bencoded list.");
        if (privateFlag is not BInt privateFlagInt)
            throw new InvalidDataException("The 'private' field is not a valid bencoded int.");

        if (length is not null && length is not BInt)
            throw new InvalidDataException();
        if (md5sum is not null && md5sum is not BString)
            throw new InvalidDataException();

        if (files is not null && files is not BList filesList)
            throw new InvalidDataException();

        var metaInfo = new MetaInfo(
            Info: new Info(
                pieceLengthInt,
                piecesString,
                privateFlagInt,
                length is null ? InfoType.Multiple : InfoType.Single,
                nameString,
                (BInt)length,
                (BString)md5sum,
                null
            ),
            Announce: announceString
        );
    }
}
