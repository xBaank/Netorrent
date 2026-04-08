namespace Netorrent.Dht;

/// <summary>
/// A DHT bootstrap node (host + port).
/// </summary>
public record DhtBootstrapNode(string Host, int Port);
