namespace Netorrent.Bencoding.Structs;

public struct BInt(long data) : IBencodingType
{
    public readonly long Data => data;

    public static implicit operator long(BInt other) => other.Data;

    public static implicit operator BInt(long data) => new(data);
}
