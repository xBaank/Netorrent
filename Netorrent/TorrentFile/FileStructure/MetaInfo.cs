using Netorrent.Bencoding.Structs;

namespace Netorrent.TorrentFile.FileStructure;

public record MetaInfo(
    Info Info,
    string Announce,
    IReadOnlyList<string[]>? AnnounceList = null,
    long? CreationDate = null,
    string? Comment = null,
    string? CreatedBy = null,
    string? Encoding = null,
    string? Title = null,
    IReadOnlyList<string>? UrlList = null
)
{
    public BDictionary ToBDictionary()
    {
        var root = new BDictionary([]);

        if (!string.IsNullOrEmpty(Announce))
        {
            root.Elements["announce"] = new BString(Announce);
        }

        if (AnnounceList is not null && AnnounceList.Count != 0)
        {
            root.Elements["announce-list"] = new BList([
                .. AnnounceList.Select(u => new BList([
                    .. u.Select(i => (IBencodingNode)new BString(i)),
                ])),
            ]);
        }

        if (UrlList is not null && UrlList.Count != 0)
        {
            root.Elements["url-list"] = new BList([
                .. UrlList.Select(u => (IBencodingNode)new BString(u)),
            ]);
        }

        if (CreationDate.HasValue)
        {
            root.Elements["creation date"] = new BInt(CreationDate.Value);
        }

        if (!string.IsNullOrEmpty(Comment))
        {
            root.Elements["comment"] = new BString(Comment);
        }

        if (!string.IsNullOrEmpty(CreatedBy))
        {
            root.Elements["created by"] = new BString(CreatedBy);
        }

        if (!string.IsNullOrEmpty(Encoding))
        {
            root.Elements["encoding"] = new BString(Encoding);
        }

        if (!string.IsNullOrEmpty(Title))
        {
            root.Elements["title"] = new BString(Title);
        }

        //We may not support BEP extensions that could modify the hash so instead of generating a BDictionary from Info Properties we use the original
        root.Elements["info"] = Info.RawInfo;

        return root;
    }
}
