using System;
using System.Buffers.Binary;
using System.Text;

namespace Netorrent.P2P.Structs;

/// <summary>
/// Represents a generic BitTorrent protocol message.
/// Each message = [length prefix][message ID][payload]
/// </summary>
internal readonly record struct Message(byte Id, byte[]? Payload)
{
    // Standard message IDs
    public const byte Choke = 0;
    public const byte Unchoke = 1;
    public const byte Interested = 2;
    public const byte NotInterested = 3;
    public const byte Have = 4;
    public const byte Bitfield = 5;
    public const byte Request = 6;
    public const byte Piece = 7;
    public const byte Cancel = 8;
    public const byte Port = 9;

    /// <summary>
    /// Keep-alive has no ID and zero length prefix.
    /// </summary>
    public static readonly Message KeepAlive = new(255, null);

    /// <summary>
    /// Serializes this message to bytes.
    /// </summary>
    public byte[] ToBytes()
    {
        if (Id == 255) // keep-alive
            return new byte[4]; // just 4 zero bytes

        int payloadLength = Payload?.Length ?? 0;
        int totalLength = 4 + 1 + payloadLength;

        var buffer = new byte[totalLength];
        BinaryPrimitives.WriteInt32BigEndian(buffer.AsSpan(0, 4), 1 + payloadLength);
        buffer[4] = Id;
        if (payloadLength > 0)
            Array.Copy(Payload!, 0, buffer, 5, payloadLength);
        return buffer;
    }

    /// <summary>
    /// Deserializes a message from a span.
    /// </summary>
    public static Message FromBytes(ReadOnlySpan<byte> data)
    {
        if (data.Length < 4)
            throw new ArgumentException("Message too short");

        int length = BinaryPrimitives.ReadInt32BigEndian(data);
        if (length == 0)
            return KeepAlive;

        if (data.Length < 4 + length)
            throw new ArgumentException("Incomplete message");

        byte id = data[4];
        byte[]? payload = length > 1 ? data.Slice(5, length - 1).ToArray() : null;

        return new Message(id, payload);
    }

    // ---- Helpers to create specific messages ----

    public static Message CreateHave(int pieceIndex)
    {
        var payload = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(payload, pieceIndex);
        return new Message(Have, payload);
    }

    public static Message CreateRequest(int index, int begin, int length)
    {
        var payload = new byte[12];
        BinaryPrimitives.WriteInt32BigEndian(payload.AsSpan(0, 4), index);
        BinaryPrimitives.WriteInt32BigEndian(payload.AsSpan(4, 4), begin);
        BinaryPrimitives.WriteInt32BigEndian(payload.AsSpan(8, 4), length);
        return new Message(Request, payload);
    }

    public static Message CreatePiece(int index, int begin, byte[] block)
    {
        var payload = new byte[8 + block.Length];
        BinaryPrimitives.WriteInt32BigEndian(payload.AsSpan(0, 4), index);
        BinaryPrimitives.WriteInt32BigEndian(payload.AsSpan(4, 4), begin);
        Array.Copy(block, 0, payload, 8, block.Length);
        return new Message(Piece, payload);
    }

    public static Message CreateBitfield(byte[] bitfield) => new(Bitfield, bitfield);

    public static Message CreateChoke() => new(Choke, null);

    public static Message CreateUnchoke() => new(Unchoke, null);

    public static Message CreateInterested() => new(Interested, null);

    public static Message CreateNotInterested() => new(NotInterested, null);

    public static Message CreateKeepAlive() => KeepAlive;
}
