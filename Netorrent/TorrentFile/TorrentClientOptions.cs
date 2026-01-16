using System.Net;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;

namespace Netorrent.TorrentFile;

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

/// <summary>
/// Options for torrent client
/// </summary>
/// <param name="HttpClient">Http client used in tracker requests</param>
/// <param name="Logger">Logger used to debug</param>
public record TorrentClientOptions(
    ILogger Logger,
    int ListenPort,
    IPAddress[] ListenAddresses,
    IPAddress[] AnnounceAddresses,
    UsedTrackers UsedTrackers
)
{
    /// <summary>
    /// Only used for testing
    /// </summary>
    internal Func<IPAddress, IPAddress>? PeerIpProxy { get; set; }

    internal TimeSpan WarmupTime { get; set; } = 8.Seconds;
};
