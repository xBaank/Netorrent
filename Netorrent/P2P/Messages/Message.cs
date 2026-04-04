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

    public static RentedArray<byte> SerializeChoke(Choke _) => SerializeSimple(IdChoke);

    public static RentedArray<byte> SerializeUnChoke(Unchoke _) => SerializeSimple(IdUnchoke);

    public static RentedArray<byte> SerializeInterested(Interested _) =>
        SerializeSimple(IdInterested);

    public static RentedArray<byte> SerializeNotInterested(NotInterested _) =>
        SerializeSimple(IdNotInterested);

    private static RentedArray<byte> SerializeSimple(byte id)
    {
        var rentedArray = new RentedArray<byte>(5);
        BinaryPrimitives.WriteInt32BigEndian(rentedArray.Memory.Span[..4], 1);
        rentedArray.Memory.Span[4] = id;
        return rentedArray;
    }

    public static RentedArray<byte> SerializeKeepAlive(KeepAlive _)
    {
        var rentedArray = new RentedArray<byte>(4);
        BinaryPrimitives.WriteInt32BigEndian(rentedArray.Memory.Span[..4], 0);
        return rentedArray;
    }

    public static RentedArray<byte> SerializeHave(Have have)
    {
        var rentedArray = new RentedArray<byte>(9);
        BinaryPrimitives.WriteInt32BigEndian(rentedArray.Memory.Span[..4], 5);
        rentedArray.Memory.Span[4] = IdHave;
        BinaryPrimitives.WriteInt32BigEndian(rentedArray.Memory.Span[5..], have.Index);
        return rentedArray;
    }

    public static RentedArray<byte> SerializePort(Port port)
    {
        var rentedArray = new RentedArray<byte>(7);
        BinaryPrimitives.WriteInt32BigEndian(rentedArray.Memory.Span[..4], 5);
        rentedArray.Memory.Span[4] = IdPort;
        BinaryPrimitives.WriteUInt16BigEndian(rentedArray.Memory.Span[5..], port.Value);
        return rentedArray;
    }

    public static RentedArray<byte> SerializeBitfield(BitfieldMessage bitfieldMessage)
    {
        using var bitfieldPayload = bitfieldMessage.Bitfield.ToRentedArray();
        var rentedArray = new RentedArray<byte>(5 + bitfieldPayload.Length);
        BinaryPrimitives.WriteInt32BigEndian(
            rentedArray.Memory.Span[..4],
            1 + bitfieldPayload.Length
        );
        rentedArray.Memory.Span[4] = IdBitfield;
        bitfieldPayload.Memory.CopyTo(rentedArray.Memory[5..]);
        return rentedArray;
    }

    public static RentedArray<byte> SerializeRequest(RequestBlockMessage m) =>
        SerializeThreeInts(IdRequest, m.Index, m.Begin, m.Length);

    public static RentedArray<byte> SerializeCancel(CancelMessage m) =>
        SerializeThreeInts(IdCancel, m.Index, m.Begin, m.Length);

    private static RentedArray<byte> SerializeThreeInts(byte id, int a, int b, int c)
    {
        var rentedArray = new RentedArray<byte>(17);
        BinaryPrimitives.WriteInt32BigEndian(rentedArray.Memory.Span[..4], 13);
        rentedArray.Memory.Span[4] = id;
        BinaryPrimitives.WriteInt32BigEndian(rentedArray.Memory.Span[5..9], a);
        BinaryPrimitives.WriteInt32BigEndian(rentedArray.Memory.Span[9..13], b);
        BinaryPrimitives.WriteInt32BigEndian(rentedArray.Memory.Span[13..17], c);
        return rentedArray;
    }

    public static RentedArray<byte> SerializeBlock(BlockMessage blockMessage)
    {
        using var payload = blockMessage.Payload;
        var rentedArray = new RentedArray<byte>(13 + payload.Length);
        BinaryPrimitives.WriteInt32BigEndian(rentedArray.Memory.Span[..4], 9 + payload.Length);
        rentedArray.Memory.Span[4] = IdPiece;
        BinaryPrimitives.WriteInt32BigEndian(rentedArray.Memory.Span[5..9], blockMessage.Index);
        BinaryPrimitives.WriteInt32BigEndian(rentedArray.Memory.Span[9..13], blockMessage.Begin);
        payload.Memory.CopyTo(rentedArray.Memory[13..]);
        return rentedArray;
    }
}
