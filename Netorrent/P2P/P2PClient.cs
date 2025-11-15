using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.IO;
using Netorrent.Other;
using Netorrent.P2P.Managers.Piece;
using Netorrent.P2P.Managers.Request;
using Netorrent.P2P.Messages;
using Netorrent.TorrentFile.FileStructure;
using TimeSpanXt;

namespace Netorrent.P2P;

internal class P2PClient : IAsyncDisposable
{
    private const int MAX_ACTIVE_PEER_COUNT = 100;
    private readonly TcpListener _listener = Tcp.GetFreeTcpListenerInRange(6881, 6899);
    private readonly MetaInfo _metaInfo;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<IPEndPoint, PeerConnection> _activePeers = [];
    private readonly ConcurrentQueue<IPEndPoint> _knownPeers = [];
    private readonly PeerId _peerId;
    private readonly Bitfield _bitField;
    private readonly ChannelReader<IPEndPoint> _trackersChannel;
    private Task? _listenerTask;
    private Task? _peersTask;

    public FileManager FileManager { get; }
    public DownloadInfo DownloadInfo { get; }

    public IPEndPoint EndPoint => (IPEndPoint)_listener.LocalEndpoint;

    public Task? ListenerTask => _listenerTask;
    public Task? PeersTask => _peersTask;

    public P2PClient(
        MetaInfo metaInfo,
        PeerId peerId,
        FileManager fileManager,
        Bitfield bitField,
        ChannelReader<IPEndPoint> trackersChannel,
        ILogger logger
    )
    {
        _peerId = peerId;
        _bitField = bitField;
        _trackersChannel = trackersChannel;
        _metaInfo = metaInfo;
        _logger = logger;
        FileManager = fileManager;
        DownloadInfo = new DownloadInfo(_activePeers, fileManager, bitField);
    }

    public void ProcessPeers(CancellationToken cancellationToken) =>
        _peersTask ??= ProcessPeersTask(cancellationToken);

    public void ListenForPeers(CancellationToken cancellationToken) =>
        _listenerTask ??= ListenTask(cancellationToken);

    private async Task ProcessPeersTask(CancellationToken cancellationToken)
    {
        var connectTasks = new List<Task>();

        await foreach (var iPEndPoint in _trackersChannel.ReadAllAsync(cancellationToken))
        {
            // Map Docker NATed IPs to 127.0.0.1 for local testing
            IPEndPoint targetEndPoint;
            if (iPEndPoint.Address.ToString().StartsWith("172."))
            {
                targetEndPoint = new IPEndPoint(IPAddress.Loopback, iPEndPoint.Port);
            }
            else
            {
                targetEndPoint = iPEndPoint;
            }

            var task = ConnectToPeerAsync(targetEndPoint, cancellationToken);
            connectTasks.Add(task);

            if (connectTasks.Count >= 100)
            {
                await Task.WhenAll(connectTasks);
                connectTasks.Clear();
            }
        }
    }

    private async Task ConnectToPeerAsync(
        IPEndPoint iPEndPoint,
        CancellationToken cancellationToken = default
    )
    {
        if (_activePeers.ContainsKey(iPEndPoint))
            return;

        if (_activePeers.Count >= MAX_ACTIVE_PEER_COUNT)
        {
            var worstPeer = GetWorstPeer();
            if (worstPeer is not null)
            {
                _activePeers.Remove(worstPeer.IPEndPoint, out _);
                await worstPeer.DisposeAsync();
            }
            else
            {
                _knownPeers.Enqueue(iPEndPoint);
                return;
            }
        }

        var client = new TcpClient();

        try
        {
            await client.ConnectAsync(iPEndPoint, cancellationToken);
        }
        catch (Exception ex)
        {
            if (_logger.IsEnabled(LogLevel.Debug))
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
            new PieceSelector(_activePeers, _bitField),
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
            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug(ex, "Error handshaking to {ip}", iPEndPoint);
            return;
        }

        if (peerConnection.PeerId == _peerId)
        {
            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation(
                    "Ignored self connection to {EndPoint}",
                    peerConnection.IPEndPoint
                );
            await peerConnection.DisposeAsync();
            return;
        }

        if (_logger.IsEnabled(LogLevel.Information))
            _logger.LogInformation("Connected to peer {PeerId}", peerConnection.PeerId);

        _activePeers[iPEndPoint] = peerConnection;
        HandlePeer(peerConnection, cancellationToken);
    }

    private async Task ListenTask(CancellationToken cancellationToken)
    {
        _listener.Start();
        while (!cancellationToken.IsCancellationRequested)
        {
            var tcpClient = await _listener.AcceptTcpClientAsync(cancellationToken);
            var remoteEndPoint = (IPEndPoint)tcpClient.Client.RemoteEndPoint!;

            if (_activePeers.ContainsKey(remoteEndPoint))
                continue;

            if (_activePeers.Count >= MAX_ACTIVE_PEER_COUNT)
            {
                var worstPeer = GetWorstPeer();
                if (worstPeer is not null)
                {
                    _activePeers.Remove(worstPeer.IPEndPoint, out _);
                    await worstPeer.DisposeAsync();
                }
                else
                {
                    _knownPeers.Enqueue(remoteEndPoint);
                    continue;
                }
            }

            var peerConnection = new PeerConnection(
                tcpClient,
                remoteEndPoint,
                _bitField,
                FileManager,
                new RequestManager(),
                new PieceManager(FileManager.MaxBlocksByPiece),
                new PieceSelector(_activePeers, _bitField),
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
                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug(ex, "Error handshaking to {ip}", peerConnection.IPEndPoint);
                await peerConnection.DisposeAsync();
                continue;
            }

            if (peerConnection.PeerId == _peerId)
            {
                if (_logger.IsEnabled(LogLevel.Information))
                    _logger.LogInformation(
                        "Ignored self connection from {EndPoint}",
                        peerConnection.IPEndPoint
                    );
                await peerConnection.DisposeAsync();
                continue;
            }

            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation("Connected from peer {PeerId}", peerConnection.PeerId);

            _activePeers[remoteEndPoint] = peerConnection;
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
                    if (_logger.IsEnabled(LogLevel.Debug))
                        _logger.LogDebug(
                            task.Exception,
                            "Exception on peer {peerId}",
                            peerConnection.PeerId
                        );
                }

                await peerConnection.DisposeAsync();
                _activePeers.Remove(peerConnection.IPEndPoint, out _);

                if (_knownPeers.TryDequeue(out var iPEndPoint))
                {
                    await ConnectToPeerAsync(iPEndPoint);
                }
            },
            CancellationToken.None,
            TaskContinuationOptions.RunContinuationsAsynchronously,
            TaskScheduler.Default
        );
    }

    private PeerConnection? GetWorstPeer()
    {
        var minAge = 30.Seconds();

        if (_activePeers.IsEmpty)
            return null;

        var avgSpeedKbps = _activePeers
            .Values.Select(p => p.SpeedTracker.CurrentBps.Kbps)
            .DefaultIfEmpty(0)
            .Average();

        if (double.IsNaN(avgSpeedKbps))
            avgSpeedKbps = 0;

        var minAcceptableSpeed = Math.Max(10, avgSpeedKbps * 0.3);

        return _activePeers
            .Values.Where(p =>
                p.ConnectionDuration > minAge && p.SpeedTracker.CurrentBps.Kbps < minAcceptableSpeed
                || p.PeerChocking
            )
            .MinBy(p => p.SpeedTracker.CurrentBps.Bps);
    }

    public async ValueTask DisposeAsync()
    {
        _listener.Stop();
        foreach (var item in _activePeers)
        {
            await item.Value.DisposeAsync();
        }
        DownloadInfo.Dispose();
    }
}
