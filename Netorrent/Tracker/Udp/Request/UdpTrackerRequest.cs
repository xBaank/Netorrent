namespace Netorrent.Tracker.Udp.Request;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using Netorrent.Other;

//TODO Use more friendly types and transform them in ToMemoryRented()
internal record UdpTrackerRequest(
    IPEndPoint IPEndPoint,
    long ConnectionId,
    int Action,
    int TransactionId,
    ReadOnlyMemory<byte> InfoHash,
    ReadOnlyMemory<byte> PeerId,
    long Downloaded,
    long Left,
    long Uploaded,
    int Event,
    int IpAddress,
    int Key,
    int NumWant,
    ushort Port
) : IUdpTrackerSendPacket
{
    private const int SIZE = 98;

    public MemoryRented<byte> ToMemoryRented()
    {
        var pool = MemoryPool<byte>.Shared.Rent(SIZE);
        var memory = pool.Memory[..SIZE];
        var span = memory.Span;

        int offset = 0;

        BinaryPrimitives.WriteInt64BigEndian(span[offset..], ConnectionId);
        offset += 8;

        BinaryPrimitives.WriteInt32BigEndian(span[offset..], Action);
        offset += 4;

        BinaryPrimitives.WriteInt32BigEndian(span[offset..], TransactionId);
        offset += 4;

        if (InfoHash.Length != 20)
            throw new ArgumentException("InfoHash must be 20 bytes", nameof(InfoHash));

        InfoHash.Span.CopyTo(span[offset..]);
        offset += 20;

        if (PeerId.Length != 20)
            throw new ArgumentException("PeerId must be 20 bytes", nameof(PeerId));

        PeerId.Span.CopyTo(span[offset..]);
        offset += 20;

        BinaryPrimitives.WriteInt64BigEndian(span[offset..], Downloaded);
        offset += 8;

        BinaryPrimitives.WriteInt64BigEndian(span[offset..], Left);
        offset += 8;

        BinaryPrimitives.WriteInt64BigEndian(span[offset..], Uploaded);
        offset += 8;

        BinaryPrimitives.WriteInt32BigEndian(span[offset..], Event);
        offset += 4;

        BinaryPrimitives.WriteInt32BigEndian(span[offset..], IpAddress);
        offset += 4;

        BinaryPrimitives.WriteInt32BigEndian(span[offset..], Key);
        offset += 4;

        BinaryPrimitives.WriteInt32BigEndian(span[offset..], NumWant);
        offset += 4;

        BinaryPrimitives.WriteUInt16BigEndian(span[offset..], Port);

        return new MemoryRented<byte>(pool, SIZE);
    }
}
