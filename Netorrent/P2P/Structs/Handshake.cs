using System;
using System.Buffers;
using System.Text;
using Lazy;

namespace Netorrent.P2P.Structs;

internal readonly record struct Handshake(
    byte Pstrlen,
    string Pstr,
    byte[] Reserved,
    byte[] InfoHash,
    byte[] PeerIdBytes
)
{
    public const string DefaultProtocol = "BitTorrent protocol";
    public const int ReservedLength = 8;
    public const int InfoHashLength = 20;
    public const int PeerIdLength = 20;
    public static int TotalLength = 49 + DefaultProtocol.Length;

    private static readonly byte[] reserved = [0, 0, 0, 0, 0, 0, 0, 0];

    [Lazy]
    public string PeerId => Encoding.ASCII.GetString(PeerIdBytes);

    /// <summary>
    /// Creates a standard BitTorrent handshake with the default protocol.
    /// </summary>
    public static Handshake Create(byte[] infoHash, byte[] peerId)
    {
        if (infoHash.Length != InfoHashLength)
            throw new ArgumentException(
                $"InfoHash must be {InfoHashLength} bytes",
                nameof(infoHash)
            );
        if (peerId.Length != PeerIdLength)
            throw new ArgumentException($"PeerId must be {PeerIdLength} bytes", nameof(peerId));

        return new Handshake(
            Pstrlen: (byte)DefaultProtocol.Length,
            Pstr: DefaultProtocol,
            Reserved: reserved,
            InfoHash: infoHash,
            PeerIdBytes: peerId
        );
    }

    /// <summary>
    /// Serializes the handshake into a byte array ready to send over TCP.
    /// </summary>
    public MemoryRented<byte> ToBytes()
    {
        var memory = MemoryPool<byte>.Shared.Rent(
            1 + Pstrlen + ReservedLength + InfoHashLength + PeerIdLength
        );

        var buffer = memory
            .Memory.Span[..(1 + Pstrlen + ReservedLength + InfoHashLength + PeerIdLength)]
            .ToArray();
        int offset = 0;

        buffer[offset] = Pstrlen;
        offset += 1;

        Encoding.ASCII.GetBytes(Pstr, 0, Pstr.Length, buffer, offset);
        offset += Pstr.Length;

        reserved.CopyTo(buffer, offset);
        offset += reserved.Length;

        InfoHash.CopyTo(buffer, offset);
        offset += InfoHash.Length;

        PeerIdBytes.CopyTo(buffer, offset);

        return new MemoryRented<byte>(memory, buffer.Length);
    }

    /// <summary>
    /// Parses a handshake message from a byte array received over TCP.
    /// </summary>
    public static Handshake FromBytes(ReadOnlySpan<byte> data)
    {
        if (data.Length < 68)
            throw new ArgumentException("Invalid handshake length");

        byte pstrlen = data[0];
        string pstr = Encoding.ASCII.GetString(data.Slice(1, pstrlen));

        int offset = 1 + pstrlen;

        byte[] reserved = data.Slice(offset, ReservedLength).ToArray();
        offset += ReservedLength;

        byte[] infoHash = data.Slice(offset, InfoHashLength).ToArray();
        offset += InfoHashLength;

        byte[] peerId = data.Slice(offset, PeerIdLength).ToArray();

        return new Handshake(pstrlen, pstr, reserved, infoHash, peerId);
    }
}
