using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
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
    Bitfield bitField,
    ILogger logger
) : IAsyncDisposable
{
    private readonly TcpListener _listener = Tcp.GetFreeTcpListenerInRange(6881, 6899);
    private readonly MetaInfo _metaInfo = metaInfo;
    private readonly ILogger _logger = logger;
    private readonly ConcurrentDictionary<IPEndPoint, PeerConnection> _knownPeers = [];
    private Task? _listenerTask;
    public FileManager FileManager { get; } = fileManager;

    public IPEndPoint EndPoint => (IPEndPoint)_listener.LocalEndpoint;
    public double TotalDownloadSpeedBps => _knownPeers.Values.Sum(p => p.SpeedTracker.CurrentBps);
    public double TotalDownloadSpeedKbps => TotalDownloadSpeedBps / 1024.0;
    public long TotalBytesDownloaded => _knownPeers.Values.Sum(p => p.SpeedTracker.TotalBytes);

    public async Task ConnectToPeerAsync(
        IPEndPoint iPEndPoint,
        CancellationToken cancellationToken = default
    )
    {
        if (_knownPeers.ContainsKey(iPEndPoint))
            return;

        var client = new TcpClient();

        try
        {
            await client.ConnectAsync(iPEndPoint, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError("Error conecting to {ip}", iPEndPoint);
            return;
        }

        var peerConnection = new PeerConnection(
            client,
            iPEndPoint,
            bitField,
            FileManager,
            new RequestManager(),
            new PieceManager(FileManager.MaxBlocksByPiece),
            new PieceSelector(_knownPeers, bitField),
            _logger
        );

        _knownPeers[iPEndPoint] = peerConnection;

        await peerConnection.PerformHandshakeAsync(
            _metaInfo.Info.InfoHash,
            peerId,
            cancellationToken
        );

        _logger.LogInformation("Connected to peer {PeerId}", peerConnection.PeerId);

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
                        new PieceSelector(_knownPeers, bitField),
                        _logger
                    );

                    _knownPeers[remoteEndPoint] = peerConnection;

                    await peerConnection.ReceiveHandshakeAsync(
                        _metaInfo.Info.InfoHash,
                        peerId,
                        cancellationToken
                    );

                    _logger.LogInformation("Connected from peer {PeerId}", peerConnection.PeerId);

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
        foreach (var item in _knownPeers)
        {
            await item.Value.DisposeAsync();
        }
    }
}
