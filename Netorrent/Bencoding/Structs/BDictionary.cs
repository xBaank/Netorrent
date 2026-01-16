namespace Netorrent.Bencoding.Structs;

public readonly struct BDictionary(Dictionary<BString, IBencodingNode> elements) : IBencodingNode
{
    public Dictionary<BString, IBencodingNode> Elements => elements;

    public static implicit operator Dictionary<BString, IBencodingNode>(BDictionary other) =>
        other.Elements;

    public static implicit operator BDictionary(Dictionary<BString, IBencodingNode> data) =>
        new(data);

    public static bool operator ==(BDictionary left, BDictionary right) => left.Equals(right);

    public static bool operator !=(BDictionary left, BDictionary right) => !(left == right);

    public override bool Equals(object? obj)
    {
        if (obj is not BDictionary other || other.Elements.Count != Elements.Count)
        {
            return false;
        }

        foreach (var kvp in Elements)
        {
            if (!other.Elements.TryGetValue(kvp.Key, out var otherVal))
            {
                return false;
            }

            if (!kvp.Value.Equals(otherVal))
            {
                return false;
            }
        }

        return true;
    }

    public override int GetHashCode()
    {
        int hash = 17;
        foreach (var kvp in Elements.OrderBy(k => k.Key.ToString()))
        {
            hash = hash * 31 + kvp.Key.GetHashCode() ^ kvp.Value.GetHashCode();
        }

        return hash;
    }
}
