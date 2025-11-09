using System.Text;

namespace Netorrent.Bencoding.Structs;

public struct BString : IBencodingNode
{
    public BString(string str)
    {
        RawData = Encoding.UTF8.GetBytes(str);
        _preloadedData = str;
    }

    public BString(byte[] bytes)
    {
        RawData = bytes;
    }

    public byte[] RawData { private set; get; }
    private string? _preloadedData = null;

    public string Data => _preloadedData ??= Encoding.UTF8.GetString(RawData);

    public static implicit operator string(BString other) => other.Data;

    public static implicit operator BString(string data) => new(data);

    public static bool operator ==(BString? left, BString? right)
    {
        if (left is null || right is null)
            return false;

        return left.Equals(right);
    }

    public static bool operator !=(BString? left, BString? right)
    {
        return !(left == right);
    }

    public override string ToString() => Data;

    public override bool Equals(object? obj)
    {
        return obj is BString @string && Data == @string.Data;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Data);
    }
}
