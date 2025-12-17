using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using Netorrent.Extensions;
using Netorrent.Other;

namespace Netorrent.Tracker.Udp.Request;

internal record UdpTrackerConnectRequest(
    IPEndPoint IPEndPoint,
    int TransactionId,
    long ProtocolId = 0x41727101980,
    int Action = 0
) : IUdpTrackerSendPacket
{
    private const int SIZE = 16;

    public RentedArray<byte> ToMemoryRented()
    {
        var array = ArrayPool<byte>.Shared.Rent(SIZE);
        var memory = array.AsMemory()[..SIZE];
        int offset = 0;

        BinaryPrimitives.WriteInt64BigEndian(memory.Span[offset..], ProtocolId);
        offset += 8;

        BinaryPrimitives.WriteInt32BigEndian(memory.Span[offset..], Action);
        offset += 4;

        BinaryPrimitives.WriteInt32BigEndian(memory.Span[offset..], TransactionId);

        return new RentedArray<byte>(array, SIZE);
    }
}
