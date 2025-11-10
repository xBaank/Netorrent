namespace Netorrent.Tracker.Udp;

internal record UdpTrackerConnectResponse(int Transactionid, long ConnectionId, int Action = 0);
