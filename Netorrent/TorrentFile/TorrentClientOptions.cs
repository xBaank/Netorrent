using System.Net;
using Microsoft.Extensions.Logging;

namespace Netorrent.TorrentFile;

public enum UsedAdressProtocol
{
    /// <summary>
    /// Create sockets with Dual Mode for both Ipv4 and Ipv6
    /// </summary>
    /// <remarks>This is the default option</remarks>
    Dual,

    /// <summary>
    /// Create sockets with Ipv4 only
    /// </summary>
    Ipv4,

    /// <summary>
    /// Create sockets with Ipv6 only
    /// </summary>
    Ipv6,
}

/// <summary>
/// Options for torrent client
/// </summary>
/// <param name="HttpClient">Http client used in tracker requests</param>
/// <param name="Logger">Logger used to debug</param>
/// <param name="ForcedIp">Forced ip to use in tracker requests</param>
public record TorrentClientOptions(
    HttpClient HttpClient,
    ILogger Logger,
    UsedAdressProtocol UsedAdressProtocol,
    IPAddress? ForcedIp
)
{
    /// <summary>
    /// Only used for testing
    /// </summary>
    internal Func<IPAddress, IPAddress>? PeerIpProxy { get; set; }
};
