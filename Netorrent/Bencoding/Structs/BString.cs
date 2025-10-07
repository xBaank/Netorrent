namespace Netorrent.Bencoding.Structs;

public struct BString(string data) : IBencodingType
{
    public readonly string Data => data;

    public static implicit operator string(BString other) => other.Data;

    public static implicit operator BString(string data) => new(data);

    public override readonly string ToString() => Data;
}
