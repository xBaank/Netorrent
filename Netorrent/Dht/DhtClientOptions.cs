using Netorrent.Extensions;

namespace Netorrent.Dht;

/// <summary>
/// Options for the DHT (BEP 5) peer discovery client.
/// </summary>
public record DhtClientOptions(
    bool Enabled,
    int Port,
    IReadOnlyList<DhtBootstrapNode> BootstrapNodes
)
{
    /// <summary>
    /// Default DHT configuration using well-known public bootstrap nodes on a random port.
    /// </summary>
    public static DhtClientOptions Default { get; } =
        new(
            Enabled: true,
            Port: 0, // bind to any available port
            BootstrapNodes:
            [
                new DhtBootstrapNode("router.bittorrent.com", 6881),
                new DhtBootstrapNode("router.utorrent.com", 6881),
                new DhtBootstrapNode("dht.transmissionbt.com", 6881),
            ]
        );

    /// <summary>Initial delay before the first get_peers query (configurable for testing).</summary>
    internal TimeSpan GetPeersDelay { get; init; } = 5.Seconds;

    /// <summary>Interval between get_peers queries (configurable for testing).</summary>
    internal TimeSpan GetPeersInterval { get; init; } = 30.Seconds;

    /// <summary>Interval between bucket refresh passes (configurable for testing).</summary>
    internal TimeSpan RefreshInterval { get; init; } = 15.Minutes;
}
