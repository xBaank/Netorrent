using System.Buffers.Binary;

namespace Netorrent.Tracker.Udp.Response;

internal record UdpTrackerScrapeResponse(
    int TransactionId,
    int Seeders,
    int Downloaded,
    int Leechers
) : IUdpTrackerReceivePacket
{
    public const int Action = 2;

    public static UdpTrackerScrapeResponse From(ReadOnlySpan<byte> data)
    {
        if (data.Length < 20)
            throw new ArgumentException("Invalid scrape response length", nameof(data));

        int transactionId = BinaryPrimitives.ReadInt32BigEndian(data.Slice(4, 4));
        int seeders = BinaryPrimitives.ReadInt32BigEndian(data.Slice(8, 4));
        int downloaded = BinaryPrimitives.ReadInt32BigEndian(data.Slice(12, 4));
        int leechers = BinaryPrimitives.ReadInt32BigEndian(data.Slice(16, 4));

        return new(transactionId, seeders, downloaded, leechers);
    }
}
