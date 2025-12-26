namespace Netorrent.TorrentFile.FileStructure;

public readonly struct InfoHash
{
    public readonly ReadOnlyMemory<byte> Data { get; }

    public InfoHash(ReadOnlyMemory<byte> infoHash)
    {
        if (infoHash.Length != 20)
        {
            throw new ArgumentOutOfRangeException(nameof(infoHash));
        }

        Data = infoHash;
    }

    public static implicit operator InfoHash(ReadOnlyMemory<byte> data) => new(data);

    public static implicit operator InfoHash(byte[] data) => new(data);

    public override bool Equals(object? obj)
    {
        return obj is InfoHash hash && Data.Span.SequenceEqual(hash.Data.Span);
    }

    public override int GetHashCode()
    {
        var hash = new HashCode();
        foreach (var b in Data.Span)
        {
            hash.Add(b);
        }
        return hash.ToHashCode();
    }

    public static bool operator ==(InfoHash left, InfoHash right)
    {
        return left.Equals(right);
    }

    public static bool operator !=(InfoHash left, InfoHash right)
    {
        return !(left == right);
    }
}
