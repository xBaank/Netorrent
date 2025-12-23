namespace Netorrent.P2P.Messages;

internal readonly struct InfoHash
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

    public static implicit operator ReadOnlyMemory<byte>(InfoHash infoHash) => infoHash.Data;

    public static implicit operator InfoHash(ReadOnlyMemory<byte> data) => new(data);

    public static implicit operator InfoHash(byte[] data) => new(data);
}
