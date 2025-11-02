using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Netorrent.IO;
using Netorrent.P2P.Managers.Request;
using Netorrent.P2P.Structs;
using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.P2P;

internal class P2PClient(
    MetaInfo metaInfo,
    string peerId,
    FileManager fileManager,
    Bitfield bitField
) : IDisposable
{
    private readonly TcpListener _listener = GetFreeTcpListenerInRange(6881, 6899);
    private readonly MetaInfo _metaInfo = metaInfo;
    public IPEndPoint EndPoint => (IPEndPoint)_listener.LocalEndpoint;

    public FileManager FileManager { get; } = fileManager;

    private readonly ConcurrentDictionary<IPEndPoint, PeerConnection> _knowPeers = [];
    private readonly List<(
        Task peerTask,
        CancellationTokenSource cancellationTokenSource
    )> peerTasks = [];

    public async Task ConnectToPeerAsync(
        IPEndPoint iPEndPoint,
        CancellationToken cancellationToken = default
    )
    {
        if (_knowPeers.ContainsKey(iPEndPoint))
            return;

        var client = new TcpClient();
        await client.ConnectAsync(iPEndPoint, cancellationToken);

        var peerConnection = new PeerConnection(
            client,
            iPEndPoint,
            bitField,
            FileManager,
            new RequestManager()
        );

        _knowPeers[iPEndPoint] = peerConnection;

        await peerConnection.PerformHandshakeAsync(
            _metaInfo.Info.InfoHash,
            peerId,
            cancellationToken
        );
        await peerConnection.SendBitfieldAsync(bitField, cancellationToken);
        var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken
        );
        var peerTask = HandlePeer(peerConnection, cancellationTokenSource.Token);
        peerTasks.Add((peerTask, cancellationTokenSource));
    }

    public async Task ListenForPeersAsync(CancellationToken cancellationToken = default)
    {
        _listener.Start();
        while (!cancellationToken.IsCancellationRequested)
        {
            var tcpClient = await _listener.AcceptTcpClientAsync(cancellationToken);
            var remoteEndPoint = (IPEndPoint)tcpClient.Client.RemoteEndPoint!;
            var peerConnection = new PeerConnection(
                tcpClient,
                remoteEndPoint,
                bitField,
                FileManager,
                new RequestManager()
            );

            _knowPeers[remoteEndPoint] = peerConnection;

            await peerConnection.ReceiveHandshakeAsync(
                _metaInfo.Info.InfoHash,
                peerId,
                cancellationToken
            );
            var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken
            );
            var peerTask = HandlePeer(peerConnection, cancellationTokenSource.Token);
            peerTasks.Add((peerTask, cancellationTokenSource));
        }
    }

    private static async Task HandlePeer(
        PeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        var outgoing = peerConnection.WriteLoop(cancellationToken);
        var incoming = peerConnection.ReadLoop(cancellationToken);
        await Task.WhenAll(outgoing, incoming);
    }

    private static TcpListener GetFreeTcpListenerInRange(int start, int end)
    {
        for (int port = start; port <= end; port++)
        {
            try
            {
                var listener = new TcpListener(IPAddress.IPv6Any, port);
                listener.Server.DualMode = true;
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
        foreach (var (_, cancellationTokenSource) in peerTasks)
        {
            cancellationTokenSource.Cancel();
        }
        foreach (var item in _knowPeers)
        {
            item.Value.Dispose();
        }
    }
}
