using System.Net;

namespace Netorrent.Tracker.Udp;

internal record UdpTrackerResponse(
    int Action,
    int TransactionId,
    int Interval,
    int Leechers,
    int Seeders,
    IReadOnlyList<PeerEndpoint> Peers
);

internal record PeerEndpoint(IPAddress IpAddress, ushort Port);
