using System.Net;
using Microsoft.Extensions.Logging;

namespace Netorrent.TorrentFile;

[Flags]
public enum UsedAddressProtocol
{
    /// <summary>
    /// Create sockets with Ipv4
    /// </summary>
    Ipv4 = 1,

    /// <summary>
    /// Create sockets with Ipv6
    /// </summary>
    Ipv6 = 2,
}

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
/// <param name="ForcedIp">Forced ip to use in tracker requests</param>
public record TorrentClientOptions(
    ILogger Logger,
    UsedAddressProtocol UsedAdressProtocol,
    UsedTrackers UsedTrackers,
    IPAddress? ForcedIp
)
{
    /// <summary>
    /// Only used for testing
    /// </summary>
    internal Func<IPAddress, IPAddress>? PeerIpProxy { get; set; }
};
