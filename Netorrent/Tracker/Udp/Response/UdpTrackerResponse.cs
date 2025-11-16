namespace Netorrent.Tracker.Udp.Response;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using Netorrent.Tracker.Udp;

internal record UdpTrackerResponse(
    int Action,
    int TransactionId,
    int Interval,
    int Leechers,
    int Seeders,
    IReadOnlyList<IPEndPoint> Peers
) : IUdpTrackerReceivePacket
{
    public static UdpTrackerResponse From(ReadOnlySpan<byte> data, AddressFamily addressFamily)
    {
        if (data.Length < 20)
            throw new ArgumentException("Invalid announce response length", nameof(data));

        int action = BinaryPrimitives.ReadInt32BigEndian(data);
        int transactionId = BinaryPrimitives.ReadInt32BigEndian(data.Slice(4, 4));
        int interval = BinaryPrimitives.ReadInt32BigEndian(data.Slice(8, 4));
        int leechers = BinaryPrimitives.ReadInt32BigEndian(data.Slice(12, 4));
        int seeders = BinaryPrimitives.ReadInt32BigEndian(data.Slice(16, 4));

        var peers = new List<IPEndPoint>();
        var offset = 20;

        if (addressFamily == AddressFamily.InterNetwork)
        {
            while (offset + 6 <= data.Length)
            {
                var ipBytes = data.Slice(offset, 4).ToArray();
                var ip = new IPAddress(ipBytes);
                var port = BinaryPrimitives.ReadUInt16BigEndian(data.Slice(offset + 4, 2));
                peers.Add(new IPEndPoint(ip, port));
                offset += 6;
            }
        }
        else if (addressFamily == AddressFamily.InterNetworkV6)
        {
            while (offset + 18 <= data.Length)
            {
                var ipBytes = data.Slice(offset, 16).ToArray();
                var ip = new IPAddress(ipBytes);
                var port = BinaryPrimitives.ReadUInt16BigEndian(data.Slice(offset + 16, 2));
                peers.Add(new IPEndPoint(ip, port));
                offset += 18;
            }
        }
        else
        {
            throw new NotSupportedException($"AddressFamily {addressFamily} is not supported");
        }

        return new UdpTrackerResponse(action, transactionId, interval, leechers, seeders, peers);
    }
}
