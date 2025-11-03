using System.Buffers;
using System.Buffers.Binary;
using Netorrent.Other;

namespace Netorrent.P2P.Messages;

/// <summary>
/// Represents a generic BitTorrent protocol message.
/// Each message = [length prefix][message ID][payload]
/// </summary>
internal readonly record struct Message(byte Id, MemoryRented<byte>? Payload) : IDisposable
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
    public MemoryRented<byte> ToBytes()
    {
        if (Id == 255) // keep-alive
        {
            var memoryOwnerKA = MemoryPool<byte>.Shared.Rent(4);
            var bufferKA = memoryOwnerKA.Memory[..4];
            return new(memoryOwnerKA, bufferKA.Length);
        }

        int payloadLength = Payload?.Memory.Length ?? 0;
        int totalLength = 4 + 1 + payloadLength;

        var memoryOwner = MemoryPool<byte>.Shared.Rent(totalLength);
        var buffer = memoryOwner.Memory[..totalLength];

        BinaryPrimitives.WriteInt32BigEndian(buffer.Span[..4], 1 + payloadLength);
        buffer.Span[4] = Id;
        if (payloadLength > 0)
            Payload!.Value.Memory.CopyTo(buffer.Slice(5, payloadLength));
        return new(memoryOwner, buffer.Length);
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

        if (length == 1)
            return new Message(id, null);
        //TODO fix memory pool
        var memoryOwner = MemoryPool<byte>.Shared.Rent(length - 1);
        var buffer = memoryOwner.Memory[..(length - 1)];

        data.Slice(5, length - 1).CopyTo(buffer.Span);

        return new Message(id, new MemoryRented<byte>(memoryOwner, buffer.Length));
    }

    // ---- Helpers to create specific messages ----

    public static Message CreateHave(int pieceIndex)
    {
        var memoryOwner = MemoryPool<byte>.Shared.Rent(4);
        var buffer = memoryOwner.Memory[..4];

        BinaryPrimitives.WriteInt32BigEndian(buffer.Span, pieceIndex);
        return new Message(Have, new MemoryRented<byte>(memoryOwner, buffer.Length));
    }

    public static Message CreateRequest(int index, int begin, int length)
    {
        var memoryOwner = MemoryPool<byte>.Shared.Rent(12);
        var buffer = memoryOwner.Memory[..12];

        BinaryPrimitives.WriteInt32BigEndian(buffer.Span[..4], index);
        BinaryPrimitives.WriteInt32BigEndian(buffer.Span.Slice(4, 4), begin);
        BinaryPrimitives.WriteInt32BigEndian(buffer.Span.Slice(8, 4), length);
        return new Message(Request, new MemoryRented<byte>(memoryOwner, buffer.Length));
    }

    public static Message CreatePiece(int index, int begin, ReadOnlySpan<byte> block)
    {
        var memoryOwner = MemoryPool<byte>.Shared.Rent(8 + block.Length);
        var buffer = memoryOwner.Memory[..block.Length];

        BinaryPrimitives.WriteInt32BigEndian(buffer.Span[..4], index);
        BinaryPrimitives.WriteInt32BigEndian(buffer.Span.Slice(4, 4), begin);
        block.CopyTo(buffer.Span[8..]);
        return new Message(Piece, new MemoryRented<byte>(memoryOwner, buffer.Length));
    }

    public static Message CreateBitfield(MemoryRented<byte> bitfield) => new(Bitfield, bitfield);

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
