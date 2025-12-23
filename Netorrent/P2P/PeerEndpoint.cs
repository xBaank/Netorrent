using System.Net;
using Netorrent.P2P.Messages;

namespace Netorrent.P2P;

public record struct PeerEndpoint(IPEndPoint EndPoint, PeerId PeerId);
