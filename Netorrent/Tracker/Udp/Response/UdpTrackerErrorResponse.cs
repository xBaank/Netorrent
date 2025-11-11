using System.Buffers.Binary;
using System.Text;

namespace Netorrent.Tracker.Udp.Response;

internal record UdpTrackerErrorResponse(int TransactionId, string Message, int Action)
    : IUdpTrackerReceivePacket
{
    public static UdpTrackerErrorResponse From(ReadOnlySpan<byte> data)
    {
        int action = BinaryPrimitives.ReadInt32BigEndian(data.Slice(0, 4));
        int transactionId = BinaryPrimitives.ReadInt32BigEndian(data.Slice(4, 4));
        string message = Encoding.UTF8.GetString(data.Slice(8));
        return new(transactionId, message, action);
    }
};
