namespace Netorrent.Bencoding.Structs;

public readonly struct BInt(long data) : IBencodingNode
{
    public long Data => data;

    public static implicit operator long(BInt other) => other.Data;

    public static implicit operator BInt(long data) => new(data);
}
