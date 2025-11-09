using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Microsoft.Extensions.Logging;
using Netorrent.IO;
using Netorrent.Other;
using Netorrent.P2P.Managers.Piece;
using Netorrent.P2P.Managers.Request;
using Netorrent.P2P.Messages;
using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.P2P;

internal class P2PClient : IAsyncDisposable
{
    private readonly TcpListener _listener = Tcp.GetFreeTcpListenerInRange(6881, 6899);
    private readonly MetaInfo _metaInfo;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<IPEndPoint, PeerConnection> _knownPeers = [];
    private readonly string _peerId;
    private readonly Bitfield _bitField;
    private Task? _listenerTask;

    public FileManager FileManager { get; }
    public DownloadInfo DownloadInfo { get; }

    public IPEndPoint EndPoint => (IPEndPoint)_listener.LocalEndpoint;

    public Task? ListenerTask => _listenerTask;

    public P2PClient(
        MetaInfo metaInfo,
        string peerId,
        FileManager fileManager,
        Bitfield bitField,
        ILogger logger
    )
    {
        _peerId = peerId;
        _bitField = bitField;
        _metaInfo = metaInfo;
        _logger = logger;
        FileManager = fileManager;
        DownloadInfo = new DownloadInfo(_knownPeers, fileManager, bitField);
    }

    public async Task ConnectToPeerAsync(
        IPEndPoint iPEndPoint,
        CancellationToken cancellationToken = default
    )
    {
        if (_knownPeers.Count > 60)
            return;

        if (_knownPeers.ContainsKey(iPEndPoint))
            return;

        var client = new TcpClient();

        try
        {
            await client.ConnectAsync(iPEndPoint, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Error conecting to {ip}", iPEndPoint);
            return;
        }

        var peerConnection = new PeerConnection(
            client,
            iPEndPoint,
            _bitField,
            FileManager,
            new RequestManager(),
            new PieceManager(FileManager.MaxBlocksByPiece),
            new PieceSelector(_knownPeers, _bitField),
            _logger
        );

        try
        {
            await peerConnection.PerformHandshakeAsync(
                _metaInfo.Info.InfoHash,
                _peerId,
                cancellationToken
            );
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Error handshaking to {ip}", iPEndPoint);
            return;
        }

        if (peerConnection.PeerId == _peerId)
        {
            _logger.LogInformation(
                "Ignored self connection to {EndPoint}",
                peerConnection.IPEndPoint
            );
            await peerConnection.DisposeAsync();
            return;
        }

        _logger.LogInformation("Connected to peer {PeerId}", peerConnection.PeerId);

        _knownPeers[iPEndPoint] = peerConnection;
        HandlePeer(peerConnection, cancellationToken);
    }

    public void ListenForPeers(CancellationToken cancellationToken = default) =>
        _listenerTask ??= ListenTask(cancellationToken);

    private async Task ListenTask(CancellationToken cancellationToken)
    {
        _listener.Start();
        while (!cancellationToken.IsCancellationRequested)
        {
            var tcpClient = await _listener.AcceptTcpClientAsync(cancellationToken);
            var remoteEndPoint = (IPEndPoint)tcpClient.Client.RemoteEndPoint!;
            var peerConnection = new PeerConnection(
                tcpClient,
                remoteEndPoint,
                _bitField,
                FileManager,
                new RequestManager(),
                new PieceManager(FileManager.MaxBlocksByPiece),
                new PieceSelector(_knownPeers, _bitField),
                _logger
            );

            try
            {
                await peerConnection.ReceiveHandshakeAsync(
                    _metaInfo.Info.InfoHash,
                    _peerId,
                    cancellationToken
                );
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error handshaking to {ip}", peerConnection.IPEndPoint);
                await peerConnection.DisposeAsync();
                return;
            }

            if (peerConnection.PeerId == _peerId)
            {
                _logger.LogInformation(
                    "Ignored self connection from {EndPoint}",
                    peerConnection.IPEndPoint
                );
                await peerConnection.DisposeAsync();
                continue;
            }

            _logger.LogInformation("Connected from peer {PeerId}", peerConnection.PeerId);

            _knownPeers[remoteEndPoint] = peerConnection;
            HandlePeer(peerConnection, cancellationToken);
        }
    }

    private void HandlePeer(PeerConnection peerConnection, CancellationToken cancellationToken)
    {
        peerConnection.Start(cancellationToken);

        peerConnection.WaitTask?.ContinueWith(
            async task =>
            {
                if (task.IsFaulted)
                {
                    _logger.LogDebug(
                        task.Exception,
                        "Exception on peer {peerId}",
                        peerConnection.PeerId
                    );
                }
                await peerConnection.DisposeAsync();
                _knownPeers.Remove(peerConnection.IPEndPoint, out _);
            },
            CancellationToken.None,
            TaskContinuationOptions.RunContinuationsAsynchronously,
            TaskScheduler.Default
        );
    }

    public async ValueTask DisposeAsync()
    {
        _listener.Stop();
        foreach (var item in _knownPeers)
        {
            await item.Value.DisposeAsync();
        }
        DownloadInfo.Dispose();
    }
}
