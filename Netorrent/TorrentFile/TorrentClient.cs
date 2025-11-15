using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using Microsoft.Extensions.Logging.Abstractions;
using Netorrent.Bencoding;
using Netorrent.Bencoding.Structs;
using Netorrent.Extensions;
using Netorrent.P2P;
using Netorrent.TorrentFile.FileStructure;
using Netorrent.Tracker.Udp;

namespace Netorrent.TorrentFile;

public class TorrentClient : IAsyncDisposable
{
    private readonly PeerId _peerId = new();
    private readonly TorrentClientOptions _options;
    private readonly List<Torrent> torrents = [];
    private readonly UdpTrackerTransactionManager _trackerTransactionManager;
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    public TorrentClient(Func<TorrentClientOptions, TorrentClientOptions>? action = null)
    {
        var options = new TorrentClientOptions(new(), NullLogger.Instance, null);
        _options = action?.Invoke(options) ?? options;
        var udpClient = new UdpClient(AddressFamily.InterNetworkV6);
        udpClient.Client.DualMode = true;
        udpClient.Client.Bind(new IPEndPoint(IPAddress.IPv6Any, 0));
        _trackerTransactionManager = new(udpClient, _options.Logger, _options.ForcedIp);
        _trackerTransactionManager.Start(_cancellationTokenSource.Token);
    }

    public async ValueTask<Torrent> ImportTorrentAsync(
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
            _options.HttpClient,
            _trackerTransactionManager,
            _peerId,
            Path.GetFullPath(outputDirectory),
            _options.Logger
        );
        torrents.Add(torrent);
        return torrent;
    }

    public Torrent ImportTorrent(MetaInfo metaInfo, string outputDirectory)
    {
        var torrent = new Torrent(
            metaInfo,
            _options.HttpClient,
            _trackerTransactionManager,
            _peerId,
            Path.GetFullPath(outputDirectory),
            _options.Logger,
            _options.ForcedIp
        );
        torrents.Add(torrent);
        return torrent;
    }

    public async ValueTask<Torrent> CreateTorrentAsync(
        string path,
        string announceUrl,
        List<string>? announceUrls = null,
        List<string>? webUrls = null,
        int pieceLength = 256 * 1024, // 256 KB default
        CancellationToken cancellationToken = default
    )
    {
        var torrent = new Torrent(
            await CreateMetaInfoFromPathAsync(
                path,
                announceUrl,
                announceUrls,
                webUrls,
                pieceLength,
                cancellationToken
            ),
            _options.HttpClient,
            _trackerTransactionManager,
            _peerId,
            Path.GetFullPath(Path.GetDirectoryName(path) ?? ""),
            _options.Logger,
            _options.ForcedIp,
            true
        );
        torrents.Add(torrent);
        return torrent;
    }

    internal static async ValueTask<MetaInfo> CreateMetaInfoFromPathAsync(
        string path,
        string announceUrl,
        List<string>? announceUrls,
        List<string>? webUrls,
        int pieceLength = 256 * 1024, // 256 KB default
        CancellationToken cancellationToken = default
    )
    {
        static bool IsValidUrl(string? url, bool allowHttp = true)
        {
            if (string.IsNullOrWhiteSpace(url))
                return false;
            if (!Uri.TryCreate(url, UriKind.Absolute, out var uri))
                return false;
            if (uri.Scheme == Uri.UriSchemeHttp && allowHttp)
                return true;
            if (uri.Scheme == Uri.UriSchemeHttps)
                return true;
            if (uri.Scheme == "udp" || uri.Scheme == "udp4" || uri.Scheme == "udp6")
                return true; // Trackers can be UDP
            return false;
        }

        if (!IsValidUrl(announceUrl))
            throw new ArgumentException($"Invalid announce URL: '{announceUrl}'");

        string[] allUrls = [.. announceUrls ?? [], .. webUrls ?? []];
        foreach (string url in allUrls)
        {
            if (!IsValidUrl(url))
            {
                throw new ArgumentException($"Invalid announce URL: '{url}'");
            }
        }

        path = Path.GetFullPath(path);

        bool isDirectory = Directory.Exists(path);
        bool isFile = File.Exists(path);
        if (!isDirectory && !isFile)
            throw new FileNotFoundException("File or directory not found.", path);

        var files = new List<(string FullPath, string RelativePath, long Length)>();

        if (isFile)
        {
            var fi = new FileInfo(path);
            files.Add((fi.FullName, fi.Name, fi.Length));
        }
        else
        {
            var root = path.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            var allFiles = Directory
                .EnumerateFiles(root, "*", SearchOption.AllDirectories)
                .Select(full => new
                {
                    Full = full,
                    Rel = full[root.Length..]
                        .TrimStart(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar),
                })
                .OrderBy(x => x.Rel, StringComparer.Ordinal)
                .ToList();

            foreach (var f in allFiles)
            {
                var fi = new FileInfo(f.Full);
                var relParts = f
                    .Rel.Split(
                        [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar],
                        StringSplitOptions.None
                    )
                    .ToList();
                files.Add((fi.FullName, string.Join("/", relParts), fi.Length));
            }

            if (files.Count == 0)
                throw new InvalidOperationException(
                    "Directory contains no files to create a torrent."
                );
        }

        var piecesBytes = new List<byte>();
        using var pool = MemoryPool<byte>.Shared.Rent(pieceLength);
        var pieceBuffer = pool.Memory[..pieceLength];
        int bufferPos = 0;

        foreach (var (fullPath, _, _) in files)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using var fs = new FileStream(
                fullPath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite,
                4096,
                useAsync: true
            );
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                int toRead = pieceLength - bufferPos;
                int bytesRead = await fs.ReadAsync(
                    pieceBuffer.Slice(bufferPos, toRead),
                    cancellationToken
                );
                if (bytesRead <= 0)
                    break;

                bufferPos += bytesRead;

                // If buffer full, hash and reset
                if (bufferPos == pieceLength)
                {
                    var pieceData = pieceBuffer[..pieceLength].ToArray();
                    var hash = SHA1.HashData(pieceData);
                    piecesBytes.AddRange(hash);
                    bufferPos = 0;
                }
            }
        }

        if (bufferPos > 0)
        {
            var lastPiece = pieceBuffer[..bufferPos].ToArray();
            var hash = SHA1.HashData(lastPiece);
            piecesBytes.AddRange(hash);
        }

        // --- Step 2: Create info dictionary ---
        var infoElements = new Dictionary<BString, IBencodingNode>
        {
            [new BString("piece length")] = new BInt(pieceLength),
            [new BString("pieces")] = new BString([.. piecesBytes]), // raw bytes
        };

        Info infoObj;
        if (isFile)
        {
            // Single-file mode
            var (FullPath, RelativePath, Length) = files[0];
            infoElements[new BString("name")] = new BString(RelativePath);
            infoElements[new BString("length")] = new BInt(Length);

            var infoDict = new BDictionary(infoElements);

            infoObj = new Info(
                RawInfo: infoDict,
                PieceLength: pieceLength,
                Pieces: [.. piecesBytes],
                Private: 0,
                Type: InfoType.Single,
                Name: RelativePath,
                Length: Length
            );
        }
        else
        {
            // Multi-file mode: build "files" list
            var filesListNodes = new List<IBencodingNode>();
            var infoFiles = new List<InfoFile>();

            foreach (var (fullPath, relPath, length) in files)
            {
                // path components are split by '/' which we set earlier
                var pathParts = relPath
                    .Split(['/'], StringSplitOptions.None)
                    .Select(p => (IBencodingNode)new BString(p))
                    .ToList();

                var fileDict = new BDictionary(
                    new Dictionary<BString, IBencodingNode>
                    {
                        [new BString("length")] = new BInt(length),
                        [new BString("path")] = new BList(pathParts),
                    }
                );

                filesListNodes.Add(fileDict);

                // Also create InfoFile instances for the Info object (adjust constructor as needed)
                infoFiles.Add(new InfoFile(length, [.. pathParts.Select(n => ((BString)n).Data)]));
            }

            infoElements[new BString("name")] = new BString(
                Path.GetFileName(Path.GetFileName(path) ?? path)
            );
            infoElements[new BString("files")] = new BList(filesListNodes);

            var infoDict = new BDictionary(infoElements);

            infoObj = new Info(
                RawInfo: infoDict,
                PieceLength: pieceLength,
                Pieces: [.. piecesBytes],
                Private: 0,
                Type: InfoType.Multiple,
                Name: Path.GetFileName(
                    path.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
                ),
                Files: infoFiles
            );
        }

        // --- Step 3: Create MetaInfo ---
        var meta = new MetaInfo(
            Info: infoObj,
            Announce: announceUrl,
            AnnounceList: announceUrls,
            UrlList: webUrls,
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

    public async ValueTask DisposeAsync()
    {
        foreach (var item in torrents)
        {
            await item.DisposeAsync();
        }
        _trackerTransactionManager.Dispose();
        _cancellationTokenSource.Cancel();
    }
}
