using System.Net;

namespace Netorrent.Dht.Routing;

internal record DhtNode(NodeId Id, IPEndPoint EndPoint)
{
    public DateTime LastSeen { get; set; } = DateTime.UtcNow;
    public bool IsBad { get; set; }
    public bool IsGood => !IsBad && DateTime.UtcNow - LastSeen < TimeSpan.FromMinutes(15);
}
