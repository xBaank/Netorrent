using System.Net;
using Microsoft.Extensions.Logging;
using Netorrent.Dht;
using Netorrent.Extensions;

namespace Netorrent.TorrentFile.Options;

/// <summary>
/// Options for torrent client
/// </summary>
/// <param name="HttpClient">Http client used in tracker requests</param>
/// <param name="Logger">Logger used to debug</param>
public record TorrentClientOptions(
    ILogger Logger,
    int ListenPort,
    IPAddress? ListenIpv4Address,
    IPAddress? ListenIpv6Address,
    UsedTrackers UsedTrackers,
    int BencodingMaxDepth = 64
)
{
    /// <summary>
    /// Options for the DHT (BEP 5) distributed peer discovery.
    /// </summary>
    public DhtClientOptions DhtOptions { get; init; } = DhtClientOptions.Default;

    /// <summary>
    /// Only used for testing
    /// </summary>
    internal Func<IPAddress, IPAddress>? PeerIpProxy { get; set; }

    internal TimeSpan WarmupTime { get; set; } = 8.Seconds;
};
