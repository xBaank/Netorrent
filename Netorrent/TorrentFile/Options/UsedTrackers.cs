namespace Netorrent.TorrentFile.Options;

[Flags]
public enum UsedTrackers
{
    /// <summary>
    /// Enables Http trackers
    /// </summary>
    Http = 1,

    /// <summary>
    /// Enables Udp Trackers
    /// </summary>
    Udp = 2,
}
