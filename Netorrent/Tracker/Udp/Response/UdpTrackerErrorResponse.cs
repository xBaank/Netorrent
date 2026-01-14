using System.Buffers.Binary;
using System.Text;

namespace Netorrent.Tracker.Udp.Response;

internal record UdpTrackerErrorResponse(int TransactionId, string Message)
    : IUdpTrackerReceivePacket
{
    public const int Action = 3;

    public static UdpTrackerErrorResponse From(ReadOnlySpan<byte> data)
    {
        int transactionId = BinaryPrimitives.ReadInt32BigEndian(data.Slice(4, 4));
        string message = Encoding.UTF8.GetString(data.Slice(8));
        return new(transactionId, message);
    }
};
