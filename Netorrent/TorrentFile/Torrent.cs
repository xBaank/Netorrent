using System.Security.Cryptography;
using System.Text;
using Netorrent.Bencoding;
using Netorrent.Bencoding.Structs;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P;
using Netorrent.TorrentFile.FileStructure;
using Netorrent.Tracker;

namespace Netorrent.TorrentFile;

public class Torrent
{
    public MetaInfo MetaInfo { get; init; }

    private readonly PeerIdService _peerIdService = new();
    private readonly HttpClient _httpClient = new();
    private readonly P2PClient _p2pClient;

    private Torrent(ReadOnlySpan<byte> data)
    {
        var decoder = new BDecoder(data);
        var decoded = decoder.Decode();
        if (decoded is not BDictionary bDictionary)
            throw new InvalidDataException("Torrent file is not a valid bencoded dictionary.");

        MetaInfo = ParseMetaInfo(bDictionary);
        _p2pClient = new P2PClient(MetaInfo, _peerIdService.PeerId);
    }

    private Torrent(MetaInfo metaInfo)
    {
        MetaInfo = metaInfo;
        _p2pClient = new P2PClient(MetaInfo, _peerIdService.PeerId);
    }

    public static async ValueTask<Torrent> AddTorrentAsync(
        string path,
        CancellationToken cancellationToken = default
    )
    {
        var torrentFileData = File.ReadAllBytesAsync(path, cancellationToken);
        var torrent = new Torrent(await torrentFileData);
        return torrent;
    }

    public static Torrent AddTorrent(MetaInfo metaInfo) => new(metaInfo);

    public static async ValueTask<Torrent> CreateTorrentAsync(
        string path,
        string announceUrl,
        List<string>? announceUrls,
        int pieceLength = 256 * 1024, // 256 KB default
        CancellationToken cancellationToken = default
    ) =>
        new Torrent(
            await CreateMetaInfoFromFileAsync(
                path,
                announceUrl,
                announceUrls,
                pieceLength,
                cancellationToken
            )
        );

    private static async ValueTask<MetaInfo> CreateMetaInfoFromFileAsync(
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
        using (var fs = File.OpenRead(path))
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
            RawInfo: infoDict,
            PieceLength: pieceLength,
            Pieces: Convert.ToHexString(piecesBytes.ToArray()), // optional string representation
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

    public async ValueTask StartAsync(CancellationToken cancellationToken = default)
    {
        var trackerClients = MetaInfo
            .AnnounceList?.Append(MetaInfo.Announce)
            .Where(url => url.StartsWith("http://") || url.StartsWith("https://"))
            ?.Select(url => new TrackerClient(
                _p2pClient,
                _httpClient,
                _peerIdService,
                MetaInfo.Info.InfoHash,
                url,
                new FilesHandler(MetaInfo)
            ))
            .ToList();

        if (trackerClients is null || trackerClients.Count == 0)
            throw new InvalidOperationException("No supported tracker URLs found.");
        try
        {
            var trakersTasks = trackerClients
                .Select(client => client.StartAsync(cancellationToken))
                .ToList();

            await Task.WhenAll(trakersTasks);
        }
        finally
        {
            foreach (var client in trackerClients)
            {
                await client.DisposeAsync();
            }
        }
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
            .Select<IBencodingNode, string>(i => i.As<BString>()!.Value)
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
                info,
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

    public static InfoFile ParseFile(IBencodingNode data)
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
