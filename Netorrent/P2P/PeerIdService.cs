using System.Security.Cryptography;

namespace Netorrent.P2P;

public class PeerIdService
{
    private readonly string _peerId;
    public string PeerId => _peerId;

    public PeerIdService()
    {
        _peerId = GeneratePeerId("NT", "1001");
    }

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
}
