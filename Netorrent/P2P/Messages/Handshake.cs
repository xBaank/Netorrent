using System.Buffers;
using System.Text;
using Netorrent.Extensions;
using Netorrent.Other;
using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.P2P.Messages;

internal readonly record struct Handshake(
    byte Pstrlen,
    string Pstr,
    InfoHash InfoHash,
    byte[] PeerIdBytes
)
{
    public const string DefaultProtocol = "BitTorrent protocol";
    public const int ReservedLength = 8;
    public const int InfoHashLength = 20;
    public const int PeerIdLength = 20;
    public static int TotalLength = 49 + DefaultProtocol.Length;

    private static readonly byte[] reserved = [0, 0, 0, 0, 0, 0, 0, 0];

    public PeerId PeerId { get; } = new(PeerIdBytes);

    /// <summary>
    /// Creates a standard BitTorrent handshake with the default protocol.
    /// </summary>
    public static Handshake Create(byte[] infoHash, byte[] peerId)
    {
        if (infoHash.Length != InfoHashLength)
        {
            throw new ArgumentException(
                $"InfoHash must be {InfoHashLength} bytes",
                nameof(infoHash)
            );
        }

        if (peerId.Length != PeerIdLength)
        {
            throw new ArgumentException($"PeerId must be {PeerIdLength} bytes", nameof(peerId));
        }

        return new Handshake(
            Pstrlen: (byte)DefaultProtocol.Length,
            Pstr: DefaultProtocol,
            InfoHash: infoHash,
            PeerIdBytes: peerId
        );
    }

    /// <summary>
    /// Serializes the handshake into a byte array ready to send over TCP.
    /// </summary>
    public RentedArray<byte> ToBytes()
    {
        var rentedArray = new RentedArray<byte>(TotalLength);
        try
        {
            var buffer = rentedArray.Memory.Span;
            int offset = 0;

            buffer[offset] = Pstrlen;
            offset += 1;

            var bytes = Encoding.ASCII.GetBytes(Pstr);
            bytes.AsSpan().CopyTo(buffer[offset..]);
            offset += Pstr.Length;

            reserved.AsSpan().CopyTo(buffer[offset..]);
            offset += reserved.Length;

            InfoHash.Data.Span.CopyTo(buffer[offset..]);
            offset += InfoHash.Data.Length;

            PeerIdBytes.AsSpan().CopyTo(buffer[offset..]);

            return rentedArray;
        }
        catch
        {
            rentedArray.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Parses a handshake message from a byte array received over TCP.
    /// </summary>
    public static Handshake FromBytes(ReadOnlySpan<byte> data)
    {
        if (data.Length < TotalLength)
        {
            throw new ArgumentException("Invalid handshake length");
        }

        byte pstrlen = data[0];
        string pstr = Encoding.ASCII.GetString(data.Slice(1, pstrlen));

        int offset = 1 + pstrlen;

        offset += ReservedLength;

        byte[] infoHash = data.Slice(offset, InfoHashLength).ToArray();
        offset += InfoHashLength;

        byte[] peerId = data.Slice(offset, PeerIdLength).ToArray();

        return new Handshake(pstrlen, pstr, infoHash, peerId);
    }
}
