namespace Netorrent.TorrentFile.FileStructure;

public record MetaInfo(
    Info Info,
    string Announce,
    List<string>? AnnounceList = null,
    long? CreationDate = null,
    string? Comment = null,
    string? CreatedBy = null,
    string? Encoding = null
);
