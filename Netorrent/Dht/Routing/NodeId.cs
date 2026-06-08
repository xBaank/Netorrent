using System.Numerics;
using System.Security.Cryptography;

namespace Netorrent.Dht.Routing;

internal readonly struct NodeId : IEquatable<NodeId>
{
    public ReadOnlyMemory<byte> Data { get; }

    public NodeId()
    {
        var bytes = new byte[20];
        RandomNumberGenerator.Fill(bytes);
        Data = bytes;
    }

    public NodeId(ReadOnlyMemory<byte> data)
    {
        if (data.Length != 20)
        {
            throw new ArgumentOutOfRangeException(nameof(data));
        }
        Data = data;
    }

    /// <summary>
    /// Returns the index (0–159) of the most significant bit in (this XOR other).
    /// Returns -1 if the two IDs are identical.
    /// </summary>
    public int BucketIndex(NodeId other)
    {
        var a = Data.Span;
        var b = other.Data.Span;
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
        var a = Data.Span;
        var b = other.Data.Span;
        var result = new byte[20];
        for (int i = 0; i < 20; i++)
            result[i] = (byte)(a[i] ^ b[i]);
        return new NodeId(result);
    }

    public int CompareTo(NodeId other)
    {
        var a = Data.Span;
        var b = other.Data.Span;
        for (int i = 0; i < 20; i++)
        {
            int diff = a[i].CompareTo(b[i]);
            if (diff != 0)
                return diff;
        }
        return 0;
    }

    public byte[] ToBytes() => Data.ToArray();

    public bool Equals(NodeId other) => Data.Span.SequenceEqual(other.Data.Span);

    public override bool Equals(object? obj) => obj is NodeId other && Equals(other);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.AddBytes(Data.Span);
        return hash.ToHashCode();
    }

    public static bool operator ==(NodeId left, NodeId right) => left.Equals(right);

    public static bool operator !=(NodeId left, NodeId right) => !left.Equals(right);
}
