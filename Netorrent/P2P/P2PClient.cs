using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Netorrent.IO;
using Netorrent.Other;
using Netorrent.P2P.Managers.Piece;
using Netorrent.P2P.Managers.Request;
using Netorrent.P2P.Messages;
using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.P2P;

internal class P2PClient(
    MetaInfo metaInfo,
    string peerId,
    FileManager fileManager,
    Bitfield bitField
) : IAsyncDisposable
{
    private readonly TcpListener _listener = Tcp.GetFreeTcpListenerInRange(6881, 6899);
    private readonly MetaInfo _metaInfo = metaInfo;
    private readonly Random _rng = new();
    private readonly ConcurrentDictionary<IPEndPoint, PeerConnection> _knowPeers = [];
    private Task? _listenerTask;
    public FileManager FileManager { get; } = fileManager;

    public IPEndPoint EndPoint => (IPEndPoint)_listener.LocalEndpoint;

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
            new RequestManager(),
            new PieceManager(FileManager.MaxBlocksByPiece),
            new PieceSelector(_knowPeers, bitField)
        );

        _knowPeers[iPEndPoint] = peerConnection;

        await peerConnection.PerformHandshakeAsync(
            _metaInfo.Info.InfoHash,
            peerId,
            cancellationToken
        );

        HandlePeer(peerConnection, cancellationToken);
    }

    public void ListenForPeers(CancellationToken cancellationToken = default) =>
        _listenerTask ??= Task.Run(
            async () =>
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
                        new RequestManager(),
                        new PieceManager(FileManager.MaxBlocksByPiece),
                        new PieceSelector(_knowPeers, bitField)
                    );

                    _knowPeers[remoteEndPoint] = peerConnection;

                    await peerConnection.ReceiveHandshakeAsync(
                        _metaInfo.Info.InfoHash,
                        peerId,
                        cancellationToken
                    );
                    HandlePeer(peerConnection, cancellationToken);
                }
            },
            cancellationToken
        );

    private void HandlePeer(PeerConnection peerConnection, CancellationToken cancellationToken)
    {
        peerConnection.Start(cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        _listener.Stop();
        foreach (var item in _knowPeers)
        {
            await item.Value.DisposeAsync();
        }
    }
}
