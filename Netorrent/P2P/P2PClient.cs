using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.P2P;

internal class P2PClient(MetaInfo metaInfo)
{
    private readonly TcpListener _listener = GetFreeTcpListenerInRange(6881, 6899);
    private readonly MetaInfo _metaInfo = metaInfo;
    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;
    private readonly ConcurrentDictionary<string, PeerConnection> _peers = [];

    public async Task ConnectToPeerAsync(
        IPEndPoint iPEndPoint,
        CancellationToken cancellationToken = default
    )
    {
        var client = new TcpClient();
        await client.ConnectAsync(iPEndPoint, cancellationToken);

        var peerConnection = new PeerConnection(client, iPEndPoint);

        //TODO Perform handshake

        //TODO use the peerId here
        _peers[iPEndPoint.Address.ToString()] = peerConnection;
    }

    static TcpListener GetFreeTcpListenerInRange(int start, int end)
    {
        for (int port = start; port <= end; port++)
        {
            try
            {
                var listener = new TcpListener(IPAddress.Any, port);
                listener.Start(); // Try to bind — this reserves the port
                return listener;
            }
            catch (SocketException)
            {
                // Port already in use — try next one
            }
        }

        throw new Exception("No free port found in the specified range.");
    }
}
