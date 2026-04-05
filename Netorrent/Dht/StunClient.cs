using System.Buffers.Binary;
using System.Net;

namespace Netorrent.Dht;

/// <summary>
/// Minimal RFC 5389 STUN Binding Request builder and response parser.
/// Only IPv4 and XOR-MAPPED-ADDRESS / MAPPED-ADDRESS are handled.
/// </summary>
internal static class StunClient
{
    private const uint MagicCookie = 0x2112A442;
    private const ushort BindingRequest = 0x0001;
    private const ushort BindingSuccessResponse = 0x0101;
    private const ushort AttrXorMappedAddress = 0x0020;
    private const ushort AttrMappedAddress = 0x0001;
    private const byte FamilyIPv4 = 0x01;

    /// <summary>
    /// Builds a 20-byte STUN Binding Request datagram with a random transaction ID.
    /// </summary>
    public static byte[] BuildRequest()
    {
        var buf = new byte[20];
        BinaryPrimitives.WriteUInt16BigEndian(buf.AsSpan(0), BindingRequest);
        BinaryPrimitives.WriteUInt16BigEndian(buf.AsSpan(2), 0); // no attributes
        BinaryPrimitives.WriteUInt32BigEndian(buf.AsSpan(4), MagicCookie);
        Random.Shared.NextBytes(buf.AsSpan(8, 12)); // 12-byte transaction ID
        return buf;
    }

    /// <summary>
    /// Returns true if <paramref name="data"/> carries the STUN magic cookie at offset 4.
    /// </summary>
    public static bool IsStunMessage(ReadOnlyMemory<byte> data) =>
        data.Length >= 20 && BinaryPrimitives.ReadUInt32BigEndian(data.Span[4..]) == MagicCookie;

    /// <summary>
    /// Parses a STUN Binding Success Response.
    /// Prefers XOR-MAPPED-ADDRESS, falls back to MAPPED-ADDRESS.
    /// Returns null if the datagram is not a valid binding success response.
    /// </summary>
    public static IPEndPoint? ParseResponse(ReadOnlyMemory<byte> data)
    {
        var span = data.Span;
        if (span.Length < 20)
            return null;

        if (BinaryPrimitives.ReadUInt16BigEndian(span) != BindingSuccessResponse)
            return null;

        var msgLength = BinaryPrimitives.ReadUInt16BigEndian(span[2..]);
        if (span.Length < 20 + msgLength)
            return null;

        int offset = 20;
        int end = 20 + msgLength;
        IPEndPoint? fallback = null;
        Span<byte> ipBuf = stackalloc byte[4];

        while (offset + 4 <= end)
        {
            var attrType = BinaryPrimitives.ReadUInt16BigEndian(span[offset..]);
            var attrLen = BinaryPrimitives.ReadUInt16BigEndian(span[(offset + 2)..]);
            int valueOffset = offset + 4;

            if (valueOffset + attrLen > span.Length)
                break;

            if (
                attrType == AttrXorMappedAddress
                && attrLen >= 8
                && span[valueOffset + 1] == FamilyIPv4
            )
            {
                // XOR port with the high 16 bits of the magic cookie
                int port =
                    BinaryPrimitives.ReadUInt16BigEndian(span[(valueOffset + 2)..])
                    ^ (int)(MagicCookie >> 16);
                uint xorIp =
                    BinaryPrimitives.ReadUInt32BigEndian(span[(valueOffset + 4)..]) ^ MagicCookie;
                BinaryPrimitives.WriteUInt32BigEndian(ipBuf, xorIp);
                return new IPEndPoint(new IPAddress(ipBuf.ToArray()), port);
            }

            if (
                attrType == AttrMappedAddress
                && attrLen >= 8
                && span[valueOffset + 1] == FamilyIPv4
                && fallback is null
            )
            {
                int port = BinaryPrimitives.ReadUInt16BigEndian(span[(valueOffset + 2)..]);
                uint ip = BinaryPrimitives.ReadUInt32BigEndian(span[(valueOffset + 4)..]);
                BinaryPrimitives.WriteUInt32BigEndian(ipBuf, ip);
                fallback = new IPEndPoint(new IPAddress(ipBuf.ToArray()), port);
            }

            // attributes are padded to 4-byte boundaries
            offset += 4 + ((attrLen + 3) & ~3);
        }

        return fallback;
    }
}
