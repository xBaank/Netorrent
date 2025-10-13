namespace Netorrent.Bencoding.Structs;

public class BDictionary(Dictionary<BString, IBencodingType> elements) : IBencodingType
{
    public Dictionary<BString, IBencodingType> Elements => elements;

    public static implicit operator Dictionary<BString, IBencodingType>(BDictionary other) =>
        other.Elements;

    public static implicit operator BDictionary(Dictionary<BString, IBencodingType> data) =>
        new(data);

    public override bool Equals(object? obj)
    {
        if (obj is not BDictionary other || other.Elements.Count != Elements.Count)
            return false;

        foreach (var kvp in Elements)
        {
            if (!other.Elements.TryGetValue(kvp.Key, out var otherVal))
                return false;

            if (!kvp.Value.Equals(otherVal))
                return false;
        }

        return true;
    }

    public override int GetHashCode()
    {
        int hash = 17;
        foreach (var kvp in Elements.OrderBy(k => k.Key.ToString()))
            hash = hash * 31 + kvp.Key.GetHashCode() ^ kvp.Value.GetHashCode();
        return hash;
    }
}
