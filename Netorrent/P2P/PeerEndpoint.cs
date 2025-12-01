using System.Net;

namespace Netorrent.P2P;

record struct PeerEndpoint(IPEndPoint IPEndPoint, PeerId PeerId);
