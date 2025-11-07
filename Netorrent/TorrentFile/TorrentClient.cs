using System.Security.Cryptography;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Netorrent.Bencoding;
using Netorrent.Bencoding.Structs;
using Netorrent.Extensions;
using Netorrent.P2P;
using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.TorrentFile;

public class TorrentClient(HttpClient? httpClient = null, ILogger? logger = null)
{
    private readonly PeerIdService _peerIdService = new();
    private readonly HttpClient _httpClient = httpClient ?? new HttpClient();
    private readonly ILogger _logger = logger ?? NullLogger.Instance;
    private readonly List<Torrent> torrents = [];

    public async ValueTask<Torrent> AddTorrentAsync(
        string path,
        string outputDirectory,
        CancellationToken cancellationToken = default
    )
    {
        var torrentFileData = await File.ReadAllBytesAsync(path, cancellationToken);

        var decoder = new BDecoder(torrentFileData);
        var decoded = decoder.Decode();
        if (decoded is not BDictionary bDictionary)
            throw new InvalidDataException("Torrent file is not a valid bencoded dictionary.");

        var metaInfo = ParseMetaInfo(bDictionary);

        var torrent = new Torrent(
            metaInfo,
            _httpClient,
            _peerIdService.PeerId,
            Path.GetFullPath(outputDirectory),
            _logger
        );
        torrents.Add(torrent);
        return torrent;
    }

    public Torrent AddTorrent(MetaInfo metaInfo, string outputDirectory)
    {
        var torrent = new Torrent(
            metaInfo,
            _httpClient,
            _peerIdService.PeerId,
            Path.GetFullPath(outputDirectory),
            _logger
        );
        torrents.Add(torrent);
        return torrent;
    }

    public async ValueTask<Torrent> CreateTorrentAsync(
        string path,
        string announceUrl,
        List<string>? announceUrls,
        int pieceLength = 256 * 1024, // 256 KB default
        CancellationToken cancellationToken = default
    )
    {
        var torrent = new Torrent(
            await CreateMetaInfoFromFileAsync(
                path,
                announceUrl,
                announceUrls,
                pieceLength,
                cancellationToken
            ),
            _httpClient,
            _peerIdService.PeerId,
            Path.GetFullPath(Path.GetDirectoryName(path) ?? ""),
            _logger,
            true
        );
        torrents.Add(torrent);
        return torrent;
    }

    internal static async ValueTask<MetaInfo> CreateMetaInfoFromFileAsync(
        string path,
        string announceUrl,
        List<string>? announceUrls,
        int pieceLength, // 256 KB default
        CancellationToken cancellationToken = default
    )
    {
        var fileInfo = new FileInfo(path);
        if (!fileInfo.Exists)
            throw new FileNotFoundException("File not found.", path);

        var fileName = fileInfo.Name;
        var fileLength = fileInfo.Length;

        // --- Step 1: Compute SHA1 hashes for each piece ---
        var piecesBytes = new List<byte>();
        using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        {
            byte[] buffer = new byte[pieceLength];
            int bytesRead;
            while ((bytesRead = await fs.ReadAsync(buffer, cancellationToken)) > 0)
            {
                byte[] chunk = buffer.AsSpan(0, bytesRead).ToArray();
                byte[] hash = SHA1.HashData(chunk);
                piecesBytes.AddRange(hash);
            }
        }

        // --- Step 2: Create info dictionary ---
        var infoDict = new BDictionary(
            new Dictionary<BString, IBencodingNode>
            {
                [new BString("name")] = new BString(fileName),
                [new BString("length")] = new BInt(fileLength),
                [new BString("piece length")] = new BInt(pieceLength),
                [new BString("pieces")] = new BString(piecesBytes.ToArray()), // raw bytes
            }
        );

        // --- Step 3: Create Info object ---
        var info = new Info(
            infoDict,
            PieceLength: pieceLength,
            Pieces: piecesBytes.ToArray(), // optional string representation
            Private: 0,
            Type: InfoType.Single,
            Name: fileName,
            Length: fileLength
        );

        // --- Step 4: Create MetaInfo ---
        var meta = new MetaInfo(
            Info: info,
            Announce: announceUrl,
            AnnounceList: announceUrls,
            CreationDate: DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
            CreatedBy: "Netorrent",
            Encoding: "UTF-8"
        );

        return meta;
    }

    internal static MetaInfo ParseMetaInfo(BDictionary dictionary)
    {
        var info =
            dictionary.Elements["info"].As<BDictionary>() ?? throw new InvalidDataException();
        var announce =
            dictionary.Elements["announce"].As<BString>() ?? throw new InvalidDataException();
        var announceList = dictionary
            .Elements.GetValueOrDefault("announce-list")
            ?.As<BList>()
            ?.Elements.Select(i => i.As<BList>()!.Value.Elements)
            .SelectMany(i => i)
            .Select<IBencodingNode, string>(i => i.As<BString>()!.Value)
            .ToList();
        var urlList = dictionary
            .Elements.GetValueOrDefault("url-list")
            ?.As<BList>()
            ?.Elements.Select<IBencodingNode, string>(i => i.As<BString>()!.Value)
            .ToList();
        var creationDate = dictionary.Elements.GetValueOrDefault("creation date")?.As<BInt>();
        var comment = dictionary.Elements.GetValueOrDefault("comment")?.As<BString>();
        var createdBy = dictionary.Elements.GetValueOrDefault("created by")?.As<BString>();
        var encoding = dictionary.Elements.GetValueOrDefault("encoding")?.As<BString>();
        var title = dictionary.Elements.GetValueOrDefault("title").As<BString>();

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
                info,
                pieceLength,
                pieces.RawData,
                privateFlag,
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
            Encoding: encoding,
            Title: title,
            UrlList: urlList
        );
    }

    internal static InfoFile ParseFile(IBencodingNode data)
    {
        var dic = data.As<BDictionary>() ?? throw new InvalidDataException();
        var length = dic.Elements["length"].As<BInt>() ?? throw new InvalidDataException();
        var path =
            dic.Elements["path"]
                .As<BList>()
                ?.Elements?.Select<IBencodingNode, string>(i =>
                    i.As<BString>() ?? throw new InvalidDataException()
                )
                ?.ToList()
            ?? throw new InvalidDataException();
        var md5sum = dic.Elements.GetValueOrDefault("md5sum")?.As<BString>();

        return new InfoFile(length, path, md5sum);
    }
}
