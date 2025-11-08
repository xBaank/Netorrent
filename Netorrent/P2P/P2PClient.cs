using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Lazy;
using Microsoft.Extensions.Logging;
using Netorrent.IO;
using Netorrent.Other;
using Netorrent.P2P.Managers;
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
    private readonly string peerId;
    private readonly Bitfield bitField;
    private readonly TaskCompletionSource _downloadTaskCompletitionSource = new();
    private Task? _listenerTask;

    public FileManager FileManager { get; }
    public IPEndPoint EndPoint => (IPEndPoint)_listener.LocalEndpoint;
    public DownloadSpeed DownloadSpeed =>
        _knownPeers.Values.Sum(p => p.SpeedTracker.CurrentBps.Bps);
    public long DownloadedBytes => _knownPeers.Values.Sum(p => p.SpeedTracker.TotalBytes);
    public IReadOnlyList<string> Peers =>
        _knownPeers.Values.Select(i => i.PeerId).Where(i => i is not null).ToList()!;
    public long TotalBytes => FileManager.TotalSize;
    public Task DownloadTask => _downloadTaskCompletitionSource.Task;

    public P2PClient(
        MetaInfo metaInfo,
        string peerId,
        FileManager fileManager,
        Bitfield bitField,
        ILogger logger
    )
    {
        this.peerId = peerId;
        this.bitField = bitField;
        _metaInfo = metaInfo;
        _logger = logger;
        FileManager = fileManager;
        bitField.OnHavePieceAsync += CheckDownload;
    }

    public async Task ConnectToPeerAsync(
        IPEndPoint iPEndPoint,
        CancellationToken cancellationToken = default
    )
    {
        if (_knownPeers.Count > 50)
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
            bitField,
            FileManager,
            new RequestManager(),
            new PieceManager(FileManager.MaxBlocksByPiece),
            new PieceSelector(_knownPeers, bitField),
            _logger
        );

        await peerConnection.PerformHandshakeAsync(
            _metaInfo.Info.InfoHash,
            peerId,
            cancellationToken
        );

        if (peerConnection.PeerId == peerId)
        {
            await peerConnection.DisposeAsync();
            _logger.LogInformation("Cant connect to self");
            return;
        }

        _logger.LogInformation("Connected to peer {PeerId}", peerConnection.PeerId);

        _knownPeers[iPEndPoint] = peerConnection;
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

                    await peerConnection.ReceiveHandshakeAsync(
                        _metaInfo.Info.InfoHash,
                        peerId,
                        cancellationToken
                    );

                    if (peerConnection.PeerId == peerId)
                    {
                        await peerConnection.DisposeAsync();
                        _logger.LogInformation("Cant connect to self");
                        continue;
                    }

                    _logger.LogInformation("Connected from peer {PeerId}", peerConnection.PeerId);

                    _knownPeers[remoteEndPoint] = peerConnection;
                    HandlePeer(peerConnection, cancellationToken);
                }
            },
            cancellationToken
        );

    private void HandlePeer(PeerConnection peerConnection, CancellationToken cancellationToken)
    {
        peerConnection.Start(cancellationToken);

        peerConnection.WaitTask.ContinueWith(
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

    private Task CheckDownload(int pieceIndex, CancellationToken token)
    {
        if (bitField.IsComplete)
        {
            _downloadTaskCompletitionSource.SetResult();
        }

        return Task.CompletedTask;
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
