namespace Netorrent.Tracker.Udp;

internal record UdpTrackerConnectRequest(
    int Transactionid,
    long ProtocolId = 0x41727101980,
    int Action = 0
);
