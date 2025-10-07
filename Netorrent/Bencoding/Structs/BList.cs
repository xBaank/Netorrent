using static System.Runtime.InteropServices.JavaScript.JSType;

namespace Netorrent.Bencoding.Structs;

public class BList(List<IBencodingType> elements) : IBencodingType
{
    public List<IBencodingType> Elements => elements;

    public static implicit operator List<IBencodingType>(BList other) => other.Elements;

    public static implicit operator BList(List<IBencodingType> data) => new(data);
}
