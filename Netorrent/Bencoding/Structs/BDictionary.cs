namespace Netorrent.Bencoding.Structs;

public class BDictionary(Dictionary<BString, IBencodingType> elements) : IBencodingType
{
    public Dictionary<BString, IBencodingType> Elements => elements;

    public static implicit operator Dictionary<BString, IBencodingType>(BDictionary other) =>
        other.Elements;

    public static implicit operator BDictionary(Dictionary<BString, IBencodingType> data) =>
        new(data);
}
