namespace Netorrent.Bencoding.Structs;

public struct BInt(int data) : IBencodingType
{
    public readonly int Data => data;

    public static implicit operator int(BInt other) => other.Data;

    public static implicit operator BInt(int data) => new(data);
}
