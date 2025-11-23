using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.Other;
using Netorrent.P2P.Download;
using Netorrent.P2P.Messages;
using Netorrent.P2P.Upload;
using Netorrent.TorrentFile.FileStructure;
using ZLinq;

namespace Netorrent.P2P;

internal class P2PClient : IAsyncDisposable
{
    private const int MAX_ACTIVE_PEER_COUNT = 50;
    private readonly TcpListener _listener = Tcp.GetFreeTcpListener();
    private readonly MetaInfo _metaInfo;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<IPEndPoint, PeerConnection> _activePeers = [];
    private readonly ConcurrentQueue<IPEndPoint> _knownPeers = [];
    private readonly PeerId _peerId;
    private readonly Bitfield _bitField;
    private readonly RequestManager _requestManager;
    private readonly ChannelReader<IPEndPoint> _trackersChannel;
    private Task? _listenerTask;
    private Task? _peersTask;
    private Func<IPAddress, IPAddress>? _peerIpProxy;

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
        ILogger logger,
        Func<IPAddress, IPAddress>? peerIpProxy
    )
    {
        _peerId = peerId;
        _bitField = bitField;
        _trackersChannel = trackersChannel;
        _metaInfo = metaInfo;
        _logger = logger;
        FileManager = fileManager;
        DownloadInfo = new DownloadInfo(_activePeers, fileManager, bitField);
        _peerIpProxy = peerIpProxy;
        _requestManager = new RequestManager(_activePeers, _bitField, fileManager);
    }

    public void ProcessPeers(CancellationToken cancellationToken) =>
        _peersTask ??= ProcessPeersTask(cancellationToken);

    public void ListenForPeers(CancellationToken cancellationToken) =>
        _listenerTask ??= ListenTask(cancellationToken);

    private async Task ProcessPeersTask(CancellationToken cancellationToken)
    {
        _requestManager.Start(cancellationToken);
        _requestManager.WaitTask?.ContinueWith(
            task =>
            {
                if (task.IsCanceled)
                {
                    DownloadInfo.SetCanceled();
                }
                else if (task.IsFaulted)
                {
                    DownloadInfo.SetException(task.Exception);
                }
            },
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default
        );

        var connectTasks = new List<Task>();

        await foreach (var iPEndPoint in _trackersChannel.ReadAllAsync(cancellationToken))
        {
            var targetEndPoint = new IPEndPoint(
                _peerIpProxy?.Invoke(iPEndPoint.Address) ?? iPEndPoint.Address,
                iPEndPoint.Port
            );

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
            new UploadScheduler(),
            _requestManager,
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
        await HandlePeer(peerConnection, cancellationToken);
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
                new UploadScheduler(),
                _requestManager,
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
            await HandlePeer(peerConnection, cancellationToken);
        }
    }

    private async Task HandlePeer(
        PeerConnection peerConnection,
        CancellationToken cancellationToken
    )
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

                _activePeers.Remove(peerConnection.IPEndPoint, out _);
                await peerConnection.DisposeAsync();

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
        //TODO if im downloading then i should get rid of peers that chock me first and then the slowest ones
        //TODO if im uploading then i should get rid of peers that i chock first and then the slowest ones
        var minAge = 30.Seconds;

        if (_activePeers.IsEmpty)
            return null;

        var avgSpeedKbps = _activePeers
            .Values.Select(p => p.DownloadSpeedTracker.CurrentBps.Kbps)
            .DefaultIfEmpty(0.0)
            .Average();

        if (double.IsNaN(avgSpeedKbps))
            avgSpeedKbps = 0.0;

        var minAcceptableSpeed = Math.Max(10.0, avgSpeedKbps * 0.3);

        var candidates = _activePeers
            .Values.AsValueEnumerable()
            .Where(p =>
                p.ConnectionDuration > minAge
                && (p.DownloadSpeedTracker.CurrentBps.Kbps < minAcceptableSpeed || p.PeerChocking)
            )
            .ToList();

        if (candidates.Count == 0)
            return null;

        return candidates.MinBy(p => p.DownloadSpeedTracker.CurrentBps.Bps);
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
