using System.Buffers.Binary;
using System.Net;
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
        var rentedArray = new RentedArray<byte>(SIZE);
        var span = rentedArray.Memory.Span;
        int offset = 0;

        BinaryPrimitives.WriteInt64BigEndian(span[offset..], ProtocolId);
        offset += 8;

        BinaryPrimitives.WriteInt32BigEndian(span[offset..], Action);
        offset += 4;

        BinaryPrimitives.WriteInt32BigEndian(span[offset..], TransactionId);

        return rentedArray;
    }
}
