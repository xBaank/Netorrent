namespace Netorrent.Bencoding.Structs;

public readonly struct BString(string data) : IBencodingType
{
    public string Data => data;

    public static implicit operator string(BString other) => other.Data;

    public static implicit operator BString(string data) => new(data);

    public override string ToString() => Data;
}
