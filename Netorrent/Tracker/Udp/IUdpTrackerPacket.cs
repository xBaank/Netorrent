using System.Net;
using Netorrent.Other;

namespace Netorrent.Tracker.Udp;

internal interface IUdpTrackerPacket
{
    int TransactionId { get; }
};

internal interface IUdpTrackerSendPacket : IUdpTrackerPacket
{
    public IPEndPoint IPEndPoint { get; }
    public RentedArray<byte> ToMemoryRented();
}

internal interface IUdpTrackerReceivePacket : IUdpTrackerPacket;
