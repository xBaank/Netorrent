using System.Net;
using Microsoft.Extensions.Logging;

namespace Netorrent.TorrentFile;

/// <summary>
/// Options for torrent client
/// </summary>
/// <param name="HttpClient">Http client used in tracker requests</param>
/// <param name="Logger">Logger used to debug</param>
/// <param name="ForcedIp">Forced ip to use in tracker requests</param>
public record TorrentClientOptions(HttpClient HttpClient, ILogger Logger, IPAddress? ForcedIp)
{
    /// <summary>
    /// Only used for testing
    /// </summary>
    internal Func<IPAddress, IPAddress>? PeerIpProxy { get; set; }
};
