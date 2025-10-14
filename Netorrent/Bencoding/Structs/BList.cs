namespace Netorrent.Bencoding.Structs;

public readonly struct BList(List<IBencodingType> elements) : IBencodingType
{
    public List<IBencodingType> Elements => elements;

    public static implicit operator List<IBencodingType>(BList other) => other.Elements;

    public static implicit operator BList(List<IBencodingType> data) => new(data);

    public static bool operator ==(BList left, BList right) => left.Equals(right);

    public static bool operator !=(BList left, BList right) => !(left == right);

    public override bool Equals(object? obj)
    {
        if (obj is not BList other || other.Elements.Count != Elements.Count)
            return false;

        for (int i = 0; i < Elements.Count; i++)
        {
            if (!Elements[i].Equals(other.Elements[i]))
                return false;
        }

        return true;
    }

    public override int GetHashCode()
    {
        int hash = 17;
        foreach (var item in Elements)
            hash = hash * 31 + item.GetHashCode();
        return hash;
    }
}
