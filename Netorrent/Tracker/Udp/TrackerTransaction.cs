namespace Netorrent.Tracker.Udp;

internal record TrackerTransaction(
    IUdpTrackerSendPacket Packet,
    TaskCompletionSource<IUdpTrackerReceivePacket> Response,
    Guid trackerId
)
{
    public int RetryCount { get; set; }
    public DateTime? NextRetryTime { get; set; }
};
