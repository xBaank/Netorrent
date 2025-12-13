using System.Net;

namespace Netorrent.P2P;

record struct PeerEndpoint(IPEndPoint EndPoint, PeerId PeerId);
