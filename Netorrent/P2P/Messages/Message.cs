using System.Buffers;
using System.Buffers.Binary;
using Netorrent.Exceptions;
using Netorrent.Other;

namespace Netorrent.P2P.Messages;

/// <summary>
/// Represents a BitTorrent protocol message.
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
    public RentedArray<byte> ToRentedArray()
    {
        if (Id == 255) // keep-alive
        {
            var arr = ArrayPool<byte>.Shared.Rent(4);
            try
            {
                // keep-alive = 4 zero bytes
                arr.AsSpan(0, 4).Clear();
                return new RentedArray<byte>(arr, 4);
            }
            catch
            {
                ArrayPool<byte>.Shared.Return(arr);
                throw;
            }
        }

        int payloadLength = Payload?.Length ?? 0;
        int length = payloadLength + 5; // 4 bytes length + 1 id byte + payload

        var buffer = ArrayPool<byte>.Shared.Rent(length);

        try
        {
            // write message length (big endian)
            BinaryPrimitives.WriteInt32BigEndian(buffer.AsSpan(0, 4), 1 + payloadLength);

            // message id
            buffer[4] = Id;

            // payload (if any)
            if (payloadLength > 0)
            {
                Payload!.Memory.CopyTo(buffer.AsMemory(5, payloadLength));
            }

            return new RentedArray<byte>(buffer, length);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(buffer);
            throw;
        }
    }

    /// <summary>
    /// Deserializes a message from a span.
    /// </summary>
    public static Message From(byte[] array, int length, byte id)
    {
        if (
            id != Choke
            && id != Unchoke
            && id != Interested
            && id != NotInterested
            && id != Have
            && id != Bitfield
            && id != Request
            && id != Piece
            && id != Cancel
            && id != Port
        )
        {
            throw new BitorrentProtocolViolationException("Invalid Id");
        }

        if (array.Length < length)
        {
            throw new BitorrentProtocolViolationException("Incomplete message");
        }

        if (length == 0)
        {
            return new Message(id, null);
        }

        if (id == Piece && length < 16 * 1024)
        {
            throw new BitorrentProtocolViolationException("Piece request must be at least 16kb");
        }

        return new Message(id, new RentedArray<byte>(array, length));
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
