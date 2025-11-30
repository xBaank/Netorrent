namespace Netorrent.Tracker.Udp;

internal record TrackerTransaction(
    IUdpTrackerSendPacket Packet,
    TaskCompletionSource<IUdpTrackerReceivePacket> Response,
    Guid TrackerId
)
{
    public int RetryCount { get; set; }
    public DateTime? NextRetryTime { get; set; }
};
