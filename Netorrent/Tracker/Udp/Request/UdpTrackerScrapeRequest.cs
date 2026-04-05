using System.Buffers.Binary;
using System.Net;
using Netorrent.Other;
using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.Tracker.Udp.Request;

internal record UdpTrackerScrapeRequest(
    IPEndPoint IPEndPoint,
    long ConnectionId,
    int TransactionId,
    InfoHash InfoHash
) : IUdpTrackerSendPacket
{
    private const int SIZE = 36;

    public RentedArray<byte> ToMemoryRented()
    {
        if (InfoHash.Data.Length != 20)
            throw new ArgumentException("InfoHash must be 20 bytes", nameof(InfoHash));

        var rentedArray = new RentedArray<byte>(SIZE);
        var span = rentedArray.Memory.Span;
        int offset = 0;

        BinaryPrimitives.WriteInt64BigEndian(span[offset..], ConnectionId);
        offset += 8;

        BinaryPrimitives.WriteInt32BigEndian(span[offset..], 2); // action = scrape
        offset += 4;

        BinaryPrimitives.WriteInt32BigEndian(span[offset..], TransactionId);
        offset += 4;

        InfoHash.Data.Span.CopyTo(span[offset..]);

        return rentedArray;
    }
}
