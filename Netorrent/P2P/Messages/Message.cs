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
            var keepAliverentedArray = new RentedArray<byte>(4);
            try
            {
                // keep-alive = 4 zero bytes
                keepAliverentedArray.Memory.Span[..4].Clear();
                return keepAliverentedArray;
            }
            catch
            {
                keepAliverentedArray.Dispose();
                throw;
            }
        }

        int payloadLength = Payload?.Length ?? 0;
        int length = payloadLength + 5; // 4 bytes length + 1 id byte + payload

        var rentedArray = new RentedArray<byte>(length);

        try
        {
            var buffer = rentedArray.Memory.Span;
            // write message length (big endian)
            BinaryPrimitives.WriteInt32BigEndian(buffer.Slice(0, 4), 1 + payloadLength);

            // message id
            buffer[4] = Id;

            // payload (if any)
            if (payloadLength > 0)
            {
                Payload!.Memory.Span.CopyTo(buffer.Slice(5, payloadLength));
            }

            return rentedArray;
        }
        catch
        {
            rentedArray.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Deserializes a message from a span.
    /// </summary>
    public static Message From(RentedArray<byte> rentedArray, byte id)
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

        if (rentedArray.Length < rentedArray.Length)
        {
            throw new BitorrentProtocolViolationException("Incomplete message");
        }

        if (rentedArray.Length == 0)
        {
            rentedArray.Dispose();
            return new Message(id, null);
        }

        if (id == Piece && rentedArray.Length < 16 * 1024)
        {
            throw new BitorrentProtocolViolationException("Piece request must be at least 16kb");
        }

        return new Message(id, rentedArray);
    }

    // ---- Helpers to create specific messages ----

    public static Message CreateHave(int pieceIndex)
    {
        var rentedArray = new RentedArray<byte>(4);
        try
        {
            var buffer = rentedArray.Memory;

            BinaryPrimitives.WriteInt32BigEndian(buffer.Span, pieceIndex);
            return new Message(Have, rentedArray);
        }
        catch
        {
            rentedArray.Dispose();
            throw;
        }
    }

    public static Message CreateRequest(int index, int begin, int length)
    {
        var rentedArray = new RentedArray<byte>(12);
        try
        {
            var buffer = rentedArray.Memory;

            BinaryPrimitives.WriteInt32BigEndian(buffer.Span[..4], index);
            BinaryPrimitives.WriteInt32BigEndian(buffer.Span.Slice(4, 4), begin);
            BinaryPrimitives.WriteInt32BigEndian(buffer.Span.Slice(8, 4), length);
            return new Message(Request, rentedArray);
        }
        catch
        {
            rentedArray.Dispose();
            throw;
        }
    }

    public static Message CreateCancel(int index, int begin, int length)
    {
        var rentedArray = new RentedArray<byte>(12);
        try
        {
            var buffer = rentedArray.Memory;

            BinaryPrimitives.WriteInt32BigEndian(buffer.Span[..4], index);
            BinaryPrimitives.WriteInt32BigEndian(buffer.Span.Slice(4, 4), begin);
            BinaryPrimitives.WriteInt32BigEndian(buffer.Span.Slice(8, 4), length);
            return new Message(Cancel, rentedArray);
        }
        catch
        {
            rentedArray.Dispose();
            throw;
        }
    }

    public static Message CreatePiece(int index, int begin, RentedArray<byte> block)
    {
        var memory = block.Memory;
        var rentedArray = new RentedArray<byte>(8 + memory.Length);
        try
        {
            var buffer = rentedArray.Memory;

            BinaryPrimitives.WriteInt32BigEndian(buffer.Span[..4], index);
            BinaryPrimitives.WriteInt32BigEndian(buffer.Span.Slice(4, 4), begin);
            memory.Span.CopyTo(buffer.Span[8..]);
            return new Message(Piece, rentedArray);
        }
        catch
        {
            rentedArray.Dispose();
            throw;
        }
    }

    public static Message CreateBitfield(RentedArray<byte> bitfieldArray) =>
        new(Bitfield, bitfieldArray);

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
