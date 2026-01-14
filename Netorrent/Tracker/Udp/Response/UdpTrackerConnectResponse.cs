using System.Buffers.Binary;

namespace Netorrent.Tracker.Udp.Response;

internal record UdpTrackerConnectResponse(int TransactionId, long ConnectionId)
    : IUdpTrackerReceivePacket
{
    public const int Action = 0;

    public static UdpTrackerConnectResponse From(ReadOnlySpan<byte> data)
    {
        var transactionId = BinaryPrimitives.ReadInt32BigEndian(data.Slice(4, 4));
        var connectionId = BinaryPrimitives.ReadInt64BigEndian(data.Slice(8, 8));
        return new(transactionId, connectionId);
    }
};
