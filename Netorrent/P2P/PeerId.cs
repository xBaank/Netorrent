using System.Security.Cryptography;
using System.Text;

namespace Netorrent.P2P;

public readonly struct PeerId
{
    public string Value { get; }

    public PeerId()
    {
        Value = GeneratePeerId("NT", "1001");
    }

    public PeerId(string value)
    {
        Value = value;
    }

    public byte[] ToBytes() => Encoding.ASCII.GetBytes(Value);

    private static string GeneratePeerId(string clientCode, string version)
    {
        // Format: -XXYYYY- + 12 random Base64-safe characters = 20 bytes
        var prefix = $"-{clientCode}{version}-";

        var randomBytes = new byte[12];
        RandomNumberGenerator.Fill(randomBytes);

        var randomPart = Convert
            .ToBase64String(randomBytes)
            .Replace('+', 'A')
            .Replace('/', 'B')
            .Replace('=', 'C')
            .Substring(0, 12);

        return prefix + randomPart;
    }

    public static bool operator ==(PeerId? obj1, PeerId? obj2) => obj1.Equals(obj2);

    public static bool operator !=(PeerId? obj1, PeerId? obj2) => !(obj1 == obj2);

    public override bool Equals(object? obj)
    {
        return obj is PeerId id && Value == id.Value;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Value);
    }

    public override string ToString() => Value;
}
