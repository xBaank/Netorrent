using System.Net;
using System.Net.Sockets;

namespace Netorrent.P2P;

public record PeerConnection(TcpClient TcpClient, IPEndPoint IPEndPoint);
