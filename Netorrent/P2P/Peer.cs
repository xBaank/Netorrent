using System.Net;

namespace Netorrent.P2P;

public record Peer(IPAddress IP, int Port);
