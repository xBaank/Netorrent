using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.P2P;

internal class P2PClient(MetaInfo metaInfo, string peerId) : IDisposable
{
    private readonly TcpListener _listener = GetFreeTcpListenerInRange(6881, 6899);
    private readonly MetaInfo _metaInfo = metaInfo;
    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;
    private readonly ConcurrentDictionary<IPEndPoint, PeerConnection> _knowPeers = [];
    private readonly ConcurrentDictionary<IPEndPoint, PeerConnection> _activePeers = [];

    public async Task ConnectToPeerAsync(
        IPEndPoint iPEndPoint,
        CancellationToken cancellationToken = default
    )
    {
        if (_knowPeers.ContainsKey(iPEndPoint))
            return;

        var client = new TcpClient();
        await client.ConnectAsync(iPEndPoint, cancellationToken);

        var peerConnection = new PeerConnection(client, iPEndPoint);
        await peerConnection.PerformHandshakeAsync(
            _metaInfo.Info.InfoHash,
            peerId,
            cancellationToken
        );

        //TODO Perform handshake

        //TODO use the peerId here
        _knowPeers[iPEndPoint] = peerConnection;
    }

    public async Task ListenForPeersAsync(CancellationToken cancellationToken = default)
    {
        _listener.Start();
        while (!cancellationToken.IsCancellationRequested)
        {
            var tcpClient = await _listener.AcceptTcpClientAsync(cancellationToken);
            var remoteEndPoint = (IPEndPoint)tcpClient.Client.RemoteEndPoint!;
            var peerConnection = new PeerConnection(tcpClient, remoteEndPoint);
            await peerConnection.PerformHandshakeAsync(
                _metaInfo.Info.InfoHash,
                peerId,
                cancellationToken
            );
            //TODO use the peerId here
            _knowPeers[remoteEndPoint] = peerConnection;
        }
    }

    private static TcpListener GetFreeTcpListenerInRange(int start, int end)
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

    public void Dispose()
    {
        _listener.Stop();
        foreach (var item in _knowPeers)
        {
            item.Value.Dispose();
        }
    }
}
