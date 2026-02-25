using System.Buffers.Binary;
using Netorrent.Other;

namespace Netorrent.P2P.Messages;

internal interface IMessage
{
    public const byte IdChoke = 0;
    public const byte IdUnchoke = 1;
    public const byte IdInterested = 2;
    public const byte IdNotInterested = 3;
    public const byte IdHave = 4;
    public const byte IdBitfield = 5;
    public const byte IdRequest = 6;
    public const byte IdPiece = 7;
    public const byte IdCancel = 8;
    public const byte IdPort = 9;

    public record Choke : IMessage
    {
        public static Choke Value { get; } = new Choke();
    };

    public record Unchoke : IMessage
    {
        public static Unchoke Value { get; } = new Unchoke();
    };

    public record Interested : IMessage
    {
        public static Interested Value { get; } = new Interested();
    };

    public record NotInterested : IMessage
    {
        public static NotInterested Value { get; } = new NotInterested();
    };

    public record KeepAlive : IMessage
    {
        public static KeepAlive Value { get; } = new KeepAlive();
    };

    public record Port(ushort Value) : IMessage;

    public record Have(int Index) : IMessage;

    public record BitfieldMessage(Bitfield Bitfield) : IMessage;

    public record BlockMessage(int Index, int Begin, RentedArray<byte> Payload)
        : IMessage,
            IDisposable
    {
        public void Dispose() => Payload.Dispose();
    }

    public record RequestBlockMessage(int Index, int Begin, int Length) : IMessage;

    public record CancelMessage(int Index, int Begin, int Length) : IMessage;

    public static RentedArray<byte> SerializeChoke(Choke _)
    {
        var rentedArray = new RentedArray<byte>(5);
        try
        {
            BinaryPrimitives.WriteInt32BigEndian(rentedArray.Memory.Span[..4], 1);
            rentedArray.Memory.Span[4] = IdChoke;
            return rentedArray;
        }
        catch
        {
            rentedArray.Dispose();
            throw;
        }
    }

    public static RentedArray<byte> SerializeUnChoke(Unchoke _)
    {
        var rentedArray = new RentedArray<byte>(5);
        try
        {
            BinaryPrimitives.WriteInt32BigEndian(rentedArray.Memory.Span[..4], 1);
            rentedArray.Memory.Span[4] = IdUnchoke;
            return rentedArray;
        }
        catch
        {
            rentedArray.Dispose();
            throw;
        }
    }

    public static RentedArray<byte> SerializeInterested(Interested _)
    {
        var rentedArray = new RentedArray<byte>(5);
        try
        {
            BinaryPrimitives.WriteInt32BigEndian(rentedArray.Memory.Span[..4], 1);
            rentedArray.Memory.Span[4] = IdInterested;
            return rentedArray;
        }
        catch
        {
            rentedArray.Dispose();
            throw;
        }
    }

    public static RentedArray<byte> SerializeNotInterested(NotInterested _)
    {
        var rentedArray = new RentedArray<byte>(5);
        try
        {
            BinaryPrimitives.WriteInt32BigEndian(rentedArray.Memory.Span[..4], 1);
            rentedArray.Memory.Span[4] = IdNotInterested;
            return rentedArray;
        }
        catch
        {
            rentedArray.Dispose();
            throw;
        }
    }

    public static RentedArray<byte> SerializeKeepAlive(KeepAlive _)
    {
        var rentedArray = new RentedArray<byte>(4);
        try
        {
            BinaryPrimitives.WriteInt32BigEndian(rentedArray.Memory.Span[..4], 0);
            return rentedArray;
        }
        catch
        {
            rentedArray.Dispose();
            throw;
        }
    }

    public static RentedArray<byte> SerializeHave(Have have)
    {
        var rentedArray = new RentedArray<byte>(9);
        try
        {
            BinaryPrimitives.WriteInt32BigEndian(rentedArray.Memory.Span[..4], 5);
            rentedArray.Memory.Span[4] = IdHave;
            BinaryPrimitives.WriteInt32BigEndian(rentedArray.Memory.Span[5..], have.Index);
            return rentedArray;
        }
        catch
        {
            rentedArray.Dispose();
            throw;
        }
    }

    public static RentedArray<byte> SerializePort(Port port)
    {
        var rentedArray = new RentedArray<byte>(7);
        try
        {
            BinaryPrimitives.WriteInt32BigEndian(rentedArray.Memory.Span[..4], 5);
            rentedArray.Memory.Span[4] = IdPort;
            BinaryPrimitives.WriteUInt16BigEndian(rentedArray.Memory.Span[5..], port.Value);
            return rentedArray;
        }
        catch
        {
            rentedArray.Dispose();
            throw;
        }
    }

    public static RentedArray<byte> SerializeBitfield(BitfieldMessage bitfieldMessage)
    {
        using var bitfieldPayload = bitfieldMessage.Bitfield.ToRentedArray();
        var rentedArray = new RentedArray<byte>(5 + bitfieldPayload.Length);
        try
        {
            BinaryPrimitives.WriteInt32BigEndian(
                rentedArray.Memory.Span[..4],
                1 + bitfieldPayload.Length
            );
            rentedArray.Memory.Span[4] = IdBitfield;
            bitfieldPayload.Memory.CopyTo(rentedArray.Memory[5..]);
            return rentedArray;
        }
        catch
        {
            rentedArray.Dispose();
            throw;
        }
    }

    public static RentedArray<byte> SerializeRequest(RequestBlockMessage requestBlockMessage)
    {
        var rentedArray = new RentedArray<byte>(17);
        try
        {
            BinaryPrimitives.WriteInt32BigEndian(rentedArray.Memory.Span[..4], 13);
            rentedArray.Memory.Span[4] = IdRequest;
            BinaryPrimitives.WriteInt32BigEndian(
                rentedArray.Memory.Span[5..9],
                requestBlockMessage.Index
            );
            BinaryPrimitives.WriteInt32BigEndian(
                rentedArray.Memory.Span[9..13],
                requestBlockMessage.Begin
            );
            BinaryPrimitives.WriteInt32BigEndian(
                rentedArray.Memory.Span[13..17],
                requestBlockMessage.Length
            );
            return rentedArray;
        }
        catch
        {
            rentedArray.Dispose();
            throw;
        }
    }

    public static RentedArray<byte> SerializeCancel(CancelMessage cancelMessage)
    {
        var rentedArray = new RentedArray<byte>(17);
        try
        {
            BinaryPrimitives.WriteInt32BigEndian(rentedArray.Memory.Span[..4], 13);
            rentedArray.Memory.Span[4] = IdCancel;
            BinaryPrimitives.WriteInt32BigEndian(
                rentedArray.Memory.Span[5..9],
                cancelMessage.Index
            );
            BinaryPrimitives.WriteInt32BigEndian(
                rentedArray.Memory.Span[9..13],
                cancelMessage.Begin
            );
            BinaryPrimitives.WriteInt32BigEndian(
                rentedArray.Memory.Span[13..17],
                cancelMessage.Length
            );
            return rentedArray;
        }
        catch
        {
            rentedArray.Dispose();
            throw;
        }
    }

    public static RentedArray<byte> SerializeBlock(BlockMessage blockMessage)
    {
        using var payload = blockMessage.Payload;
        var rentedArray = new RentedArray<byte>(13 + payload.Length);
        try
        {
            BinaryPrimitives.WriteInt32BigEndian(rentedArray.Memory.Span[..4], 9 + payload.Length);
            rentedArray.Memory.Span[4] = IdPiece;
            BinaryPrimitives.WriteInt32BigEndian(rentedArray.Memory.Span[5..9], blockMessage.Index);
            BinaryPrimitives.WriteInt32BigEndian(
                rentedArray.Memory.Span[9..13],
                blockMessage.Begin
            );
            payload.Memory.CopyTo(rentedArray.Memory[13..]);
            return rentedArray;
        }
        catch
        {
            rentedArray.Dispose();
            throw;
        }
    }
}
