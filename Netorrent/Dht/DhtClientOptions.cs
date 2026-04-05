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

    /// <summary>
    /// STUN servers used to discover the external UDP endpoint when behind NAT.
    /// Set to an empty list to disable STUN.
    /// </summary>
    public IReadOnlyList<DhtBootstrapNode> StunServers { get; init; } =
    [new("stun.l.google.com", 19302), new("stun1.l.google.com", 19302)];

    /// <summary>Initial delay before the first get_peers query (configurable for testing).</summary>
    internal TimeSpan GetPeersDelay { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Interval between get_peers queries (configurable for testing).</summary>
    internal TimeSpan GetPeersInterval { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Interval between bucket refresh passes (configurable for testing).</summary>
    internal TimeSpan RefreshInterval { get; init; } = TimeSpan.FromMinutes(15);
}

/// <summary>
/// A DHT bootstrap node (host + port).
/// </summary>
public record DhtBootstrapNode(string Host, int Port);
