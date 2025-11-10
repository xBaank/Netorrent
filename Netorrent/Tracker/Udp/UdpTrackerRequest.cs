namespace Netorrent.Tracker.Udp;

internal record UdpTrackerRequest(
    long ConnectionId,
    int Action,
    int TransactionId,
    ReadOnlyMemory<byte> InfoHash,
    long Downloaded,
    long Left,
    long Uploaded,
    int Event,
    int IpAddress,
    int Key,
    int NumWant,
    ushort Port
);
