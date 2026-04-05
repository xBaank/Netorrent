using System.Numerics;
using System.Security.Cryptography;
using Netorrent.P2P.Messages;
using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.Dht.Routing;

internal readonly struct NodeId : IEquatable<NodeId>
{
    private static readonly byte[] _zeroBytes = new byte[20];

    private readonly byte[]? _data;

    private NodeId(byte[] data)
    {
        _data = data;
    }

    private byte[] SafeData => _data ?? _zeroBytes;

    public static NodeId Generate()
    {
        var bytes = new byte[20];
        RandomNumberGenerator.Fill(bytes);
        return new NodeId(bytes);
    }

    public static NodeId FromInfoHash(InfoHash infoHash)
    {
        var bytes = infoHash.Data.ToArray();
        return new NodeId(bytes);
    }

    public static NodeId FromPeerId(PeerId peerId)
    {
        var bytes = peerId.Bytes.ToArray();
        return new NodeId(bytes);
    }

    public static NodeId FromBytes(byte[] bytes)
    {
        if (bytes.Length != 20)
            throw new ArgumentException("NodeId must be exactly 20 bytes.", nameof(bytes));
        return new NodeId(bytes);
    }

    /// <summary>
    /// Returns the index (0–159) of the most significant bit in (this XOR other).
    /// Returns -1 if the two IDs are identical.
    /// </summary>
    public int BucketIndex(NodeId other)
    {
        var a = SafeData;
        var b = other.SafeData;
        for (int i = 0; i < 20; i++)
        {
            var xorByte = (byte)(a[i] ^ b[i]);
            if (xorByte != 0)
            {
                // BitOperations.LeadingZeroCount treats the value as uint (padded to 32 bits).
                // For a byte value, leading zeros in uint = 24 + leading zeros in the byte.
                int leadingInByte = (int)BitOperations.LeadingZeroCount((uint)xorByte) - 24;
                return i * 8 + leadingInByte;
            }
        }
        return -1;
    }

    public NodeId Xor(NodeId other)
    {
        var a = SafeData;
        var b = other.SafeData;
        var result = new byte[20];
        for (int i = 0; i < 20; i++)
            result[i] = (byte)(a[i] ^ b[i]);
        return new NodeId(result);
    }

    public int CompareTo(NodeId other)
    {
        var a = SafeData;
        var b = other.SafeData;
        for (int i = 0; i < 20; i++)
        {
            int diff = a[i].CompareTo(b[i]);
            if (diff != 0)
                return diff;
        }
        return 0;
    }

    public byte[] ToBytes() => SafeData.ToArray();

    public bool Equals(NodeId other) => SafeData.AsSpan().SequenceEqual(other.SafeData.AsSpan());

    public override bool Equals(object? obj) => obj is NodeId other && Equals(other);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.AddBytes(SafeData);
        return hash.ToHashCode();
    }

    public static bool operator ==(NodeId left, NodeId right) => left.Equals(right);

    public static bool operator !=(NodeId left, NodeId right) => !left.Equals(right);
}

internal sealed class DhtNode(NodeId id, System.Net.IPEndPoint endPoint)
{
    public NodeId Id { get; } = id;
    public System.Net.IPEndPoint EndPoint { get; } = endPoint;
    public DateTime LastSeen { get; set; } = DateTime.UtcNow;
    public bool IsBad { get; set; }
    public bool IsGood => !IsBad && DateTime.UtcNow - LastSeen < TimeSpan.FromMinutes(15);
}
