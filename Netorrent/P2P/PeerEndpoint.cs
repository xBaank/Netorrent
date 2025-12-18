using System.Net;

namespace Netorrent.P2P;

public record struct PeerEndpoint(IPEndPoint EndPoint, PeerId PeerId);
