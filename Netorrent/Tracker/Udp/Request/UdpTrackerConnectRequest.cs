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
        var rentedArray = new RentedArray<byte>(SIZE);
        try
        {
            var memory = rentedArray.Memory;
            int offset = 0;

            BinaryPrimitives.WriteInt64BigEndian(memory.Span[offset..], ProtocolId);
            offset += 8;

            BinaryPrimitives.WriteInt32BigEndian(memory.Span[offset..], Action);
            offset += 4;

            BinaryPrimitives.WriteInt32BigEndian(memory.Span[offset..], TransactionId);

            return rentedArray;
        }
        catch
        {
            rentedArray.Dispose();
            throw;
        }
    }
}
