using System.Net;
using System.Security.Cryptography;

namespace Netorrent.Dht.Krpc;

/// <summary>
/// Manages announce tokens for DHT. Rotates a shared secret every 5 minutes.
/// Tokens are SHA-1(secret || ip_bytes), valid for ~10 minutes (two rotation periods).
/// </summary>
internal sealed class KrpcTokenStore
{
    private byte[] _currentSecret = GenerateSecret();
    private byte[] _previousSecret = GenerateSecret();

    private static byte[] GenerateSecret()
    {
        var secret = new byte[8];
        RandomNumberGenerator.Fill(secret);
        return secret;
    }

    public byte[] GenerateToken(IPAddress remoteAddress) =>
        ComputeToken(_currentSecret, remoteAddress);

    public bool ValidateToken(IPAddress remoteAddress, byte[] token)
    {
        var expected = ComputeToken(_currentSecret, remoteAddress);
        if (token.AsSpan().SequenceEqual(expected.AsSpan()))
            return true;

        var expectedPrev = ComputeToken(_previousSecret, remoteAddress);
        return token.AsSpan().SequenceEqual(expectedPrev.AsSpan());
    }

    public void RotateSecret()
    {
        _previousSecret = _currentSecret;
        _currentSecret = GenerateSecret();
    }

    private static byte[] ComputeToken(byte[] secret, IPAddress address)
    {
        var ipBytes = address.GetAddressBytes();
        var input = new byte[secret.Length + ipBytes.Length];
        secret.CopyTo(input, 0);
        ipBytes.CopyTo(input, secret.Length);
        return SHA1.HashData(input);
    }
}
