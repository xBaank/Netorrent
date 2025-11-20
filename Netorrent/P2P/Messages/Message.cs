using System.Buffers;
using System.Buffers.Binary;
using Netorrent.Other;

namespace Netorrent.P2P.Messages;

/// <summary>
/// Represents a generic BitTorrent protocol message.
/// Each message = [length prefix][message ID][payload]
/// </summary>
internal readonly record struct Message(byte Id, RentedArray<byte>? Payload) : IDisposable
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
    public RentedArray<byte> ToMemoryRented()
    {
        if (Id == 255) // keep-alive
        {
            return new(ArrayPool<byte>.Shared.Rent(4), 4);
        }

        int payloadLength = Payload?.Memory.Length ?? 0;
        int totalLength = 4 + 1 + payloadLength;

        var array = ArrayPool<byte>.Shared.Rent(totalLength);
        var buffer = array.AsSpan()[..totalLength];

        BinaryPrimitives.WriteInt32BigEndian(buffer[..4], 1 + payloadLength);
        buffer[4] = Id;
        if (payloadLength > 0)
            Payload!.Memory.Span.CopyTo(buffer.Slice(5, payloadLength));
        return new(array, buffer.Length);
    }

    /// <summary>
    /// Deserializes a message from a span.
    /// </summary>
    public static Message From(byte[] array, int memoryLength)
    {
        var data = array.AsMemory()[..memoryLength];
        if (data.Length < 4)
            throw new ArgumentException("Message too short");

        int length = BinaryPrimitives.ReadInt32BigEndian(data.Span);
        if (length == 0)
            return KeepAlive;

        if (data.Length < 4 + length)
            throw new ArgumentException("Incomplete message");

        byte id = data.Span[4];

        if (length == 1)
            return new Message(id, null);

        return new Message(id, new RentedArray<byte>(array, length - 1, 5));
    }

    // ---- Helpers to create specific messages ----

    public static Message CreateHave(int pieceIndex)
    {
        var memoryOwner = ArrayPool<byte>.Shared.Rent(4);
        var buffer = memoryOwner.AsMemory()[..4];

        BinaryPrimitives.WriteInt32BigEndian(buffer.Span, pieceIndex);
        return new Message(Have, new RentedArray<byte>(memoryOwner, buffer.Length));
    }

    public static Message CreateRequest(int index, int begin, int length)
    {
        var array = ArrayPool<byte>.Shared.Rent(12);
        var buffer = array.AsMemory()[..12];

        BinaryPrimitives.WriteInt32BigEndian(buffer.Span[..4], index);
        BinaryPrimitives.WriteInt32BigEndian(buffer.Span.Slice(4, 4), begin);
        BinaryPrimitives.WriteInt32BigEndian(buffer.Span.Slice(8, 4), length);
        return new Message(Request, new RentedArray<byte>(array, buffer.Length));
    }

    public static Message CreateCancel(int index, int begin, int length)
    {
        var array = ArrayPool<byte>.Shared.Rent(12);
        var buffer = array.AsMemory()[..12];

        BinaryPrimitives.WriteInt32BigEndian(buffer.Span[..4], index);
        BinaryPrimitives.WriteInt32BigEndian(buffer.Span.Slice(4, 4), begin);
        BinaryPrimitives.WriteInt32BigEndian(buffer.Span.Slice(8, 4), length);
        return new Message(Cancel, new RentedArray<byte>(array, buffer.Length));
    }

    public static Message CreatePiece(int index, int begin, RentedArray<byte> block)
    {
        var memory = block.Memory;
        var memoryOwner = ArrayPool<byte>.Shared.Rent(8 + memory.Length);
        var buffer = memoryOwner.AsMemory()[..(8 + memory.Length)];

        BinaryPrimitives.WriteInt32BigEndian(buffer.Span[..4], index);
        BinaryPrimitives.WriteInt32BigEndian(buffer.Span.Slice(4, 4), begin);
        memory.Span.CopyTo(buffer.Span[8..]);
        return new Message(Piece, new RentedArray<byte>(memoryOwner, buffer.Length));
    }

    public static Message CreateBitfield(RentedArray<byte> bitfield) => new(Bitfield, bitfield);

    public static Message CreateChoke() => new(Choke, null);

    public static Message CreateUnchoke() => new(Unchoke, null);

    public static Message CreateInterested() => new(Interested, null);

    public static Message CreateNotInterested() => new(NotInterested, null);

    public static Message CreateKeepAlive() => KeepAlive;

    public void Dispose()
    {
        Payload?.Dispose();
    }
}
