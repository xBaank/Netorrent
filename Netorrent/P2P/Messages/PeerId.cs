using System.Security.Cryptography;
using System.Text;

namespace Netorrent.P2P.Messages;

public readonly struct PeerId
{
    private static readonly byte[] _clientCode = Encoding.ASCII.GetBytes("-NT");
    private static readonly byte[] _version = Encoding.ASCII.GetBytes("1001-");

    public ReadOnlyMemory<byte> Bytes { get; }
    public string Value { get; }

    public PeerId()
    {
        Bytes = GeneratePeerId();
        Value = Encoding.ASCII.GetString(Bytes.Span);
    }

    public PeerId(ReadOnlyMemory<byte> value)
    {
        if (value.Length != 20)
        {
            throw new ArgumentOutOfRangeException(nameof(value), "Peer id must be 20 bytes");
        }

        Bytes = value;
        Value = Encoding.ASCII.GetString(Bytes.Span);
    }

    public byte[] ToBytes() => Encoding.ASCII.GetBytes(Value);

    private static ReadOnlyMemory<byte> GeneratePeerId()
    {
        Memory<byte> data = new byte[20];
        _clientCode.CopyTo(data);
        _version.CopyTo(data[_clientCode.Length..]);
        var toFill = data[(_clientCode.Length + _version.Length)..];
        RandomNumberGenerator.Fill(toFill.Span);
        return data;
    }

    public static bool operator ==(PeerId? obj1, PeerId? obj2) => obj1.Equals(obj2);

    public static bool operator !=(PeerId? obj1, PeerId? obj2) => !(obj1 == obj2);

    public override bool Equals(object? obj)
    {
        return obj is PeerId peerId && Value == peerId.Value;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Value);
    }

    public override string ToString() => Value;
}
