using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;
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
    const int MAX_ACTIVE_PEER_COUNT = 50;
    const int PEER_TIMEOUT_SECONDS = 120;

    private readonly TcpListener _listener = Tcp.GetFreeTcpListener();
    private readonly MetaInfo _metaInfo;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<PeerEndpoint, PeerConnection> _activePeers = [];
    private readonly ConcurrentQueue<IPEndPoint> _knownPeers = [];
    private readonly PeerId _peerId;
    private readonly Bitfield _bitField;
    private readonly RequestScheduler _requestManager;
    private readonly UploadScheduler _uploadScheduler;
    private readonly ChannelReader<IPEndPoint> _trackersChannel;
    private readonly Func<IPAddress, IPAddress>? _peerIpProxy;
    private readonly SemaphoreSlim _semaphoreSlim = new(1);
    private readonly List<Task> _peerTasks = [];

    public FileManager FileManager { get; }
    public DownloadInfo DownloadInfo { get; }
    public IPEndPoint EndPoint => (IPEndPoint)_listener.LocalEndpoint;

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
        _requestManager = new RequestScheduler(
            _activePeers,
            _bitField,
            new PiecePicker(_bitField, fileManager),
            logger
        );
        _uploadScheduler = new UploadScheduler(fileManager, logger);
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        await cts.CancelOnFirstCompletionAndAwaitAllAsync([
                _requestManager.StartAsync(cts.Token),
                _uploadScheduler.StartAsync(cts.Token),
                ProcessPeersAsync(cts.Token),
                ListenToPeersAsync(cts.Token),
            ])
            .ConfigureAwait(false);

        await Task.WhenAll(_peerTasks).ConfigureAwait(false);
    }

    private async Task ProcessPeersAsync(CancellationToken cancellationToken)
    {
        List<Task> connectTasks = new(100);
        await foreach (
            var iPEndPoint in _trackersChannel.ReadAllAsync(cancellationToken).ConfigureAwait(false)
        )
        {
            var targetEndPoint = new IPEndPoint(
                _peerIpProxy?.Invoke(iPEndPoint.Address) ?? iPEndPoint.Address,
                iPEndPoint.Port
            );
            connectTasks.Add(ConnectToPeerAsync(null, targetEndPoint, cancellationToken));

            if (connectTasks.Count >= 100)
            {
                await Task.WhenAll(connectTasks).ConfigureAwait(false);
            }
        }
        await Task.WhenAll(connectTasks).ConfigureAwait(false);
    }

    private async Task ListenToPeersAsync(CancellationToken cancellationToken)
    {
        _listener.Start();
        while (!cancellationToken.IsCancellationRequested)
        {
            var tcpClient = await _listener
                .AcceptTcpClientAsync(cancellationToken)
                .ConfigureAwait(false);
            var remoteEndPoint = (IPEndPoint)tcpClient.Client.RemoteEndPoint!;

            await ConnectToPeerAsync(tcpClient, remoteEndPoint, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private async Task ConnectToPeerAsync(
        TcpClient? client,
        IPEndPoint iPEndPoint,
        CancellationToken cancellationToken = default
    )
    {
        var amInitiating = client is null;

        //If im initiaing the connection
        if (amInitiating)
        {
            client = new TcpClient();

            try
            {
                using var cts = cancellationToken.WithTimeout(10.Seconds);
                await client.ConnectAsync(iPEndPoint, cts.Token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug(ex, "Error conecting to {ip}", iPEndPoint);
                return;
            }
        }

        var peerConnection = new PeerConnection(
            iPEndPoint,
            _bitField,
            _uploadScheduler,
            _requestManager,
            new MessageStream(client!.GetStream(), PEER_TIMEOUT_SECONDS.Seconds)
        );

        try
        {
            if (amInitiating)
            {
                await peerConnection
                    .PerformHandshakeAsync(_metaInfo.Info.InfoHash, _peerId, cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                await peerConnection
                    .ReceiveHandshakeAsync(_metaInfo.Info.InfoHash, _peerId, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug(ex, "Error handshaking to {ip}", iPEndPoint);
            return;
        }

        await _semaphoreSlim.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (peerConnection.PeerId == _peerId)
            {
                if (_logger.IsEnabled(LogLevel.Information))
                    _logger.LogInformation(
                        "Ignored self connection to {EndPoint}",
                        peerConnection.IPEndPoint
                    );
                await peerConnection.DisposeAsync().ConfigureAwait(false);
                return;
            }

            if (
                _activePeers.ContainsKey(peerConnection.PeerEndpoint)
                || _activePeers.Keys.AsValueEnumerable().Any(i => i.PeerId == peerConnection.PeerId)
            )
            {
                if (_logger.IsEnabled(LogLevel.Information))
                    _logger.LogInformation(
                        "Ignored active peer from {EndPoint}",
                        peerConnection.IPEndPoint
                    );
                await peerConnection.DisposeAsync().ConfigureAwait(false);
                return;
            }

            if (_activePeers.Count >= MAX_ACTIVE_PEER_COUNT)
            {
                var worstPeer = GetWorstPeer();
                if (worstPeer is not null)
                {
                    _activePeers.Remove(worstPeer.PeerEndpoint, out _);
                    await worstPeer.DisposeAsync().ConfigureAwait(false);
                }
                else
                {
                    _knownPeers.Enqueue(iPEndPoint);
                    return;
                }
            }

            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation("Connected to peer {PeerId}", peerConnection.PeerId);

            _activePeers[peerConnection.PeerEndpoint] = peerConnection;
            _peerTasks.Add(HandlePeer(peerConnection, cancellationToken));
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    private async Task HandlePeer(
        PeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        try
        {
            await peerConnection.StartAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            await peerConnection.DisposeAsync().ConfigureAwait(false);
            return;
        }
        catch (Exception ex)
        {
            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug(ex, "Exception on peer {peerId}", peerConnection.PeerId);
            }

            _activePeers.Remove(peerConnection.PeerEndpoint, out _);
            await peerConnection.DisposeAsync().ConfigureAwait(false);

            if (_knownPeers.TryDequeue(out var nextEndpoint))
            {
                await ConnectToPeerAsync(null, nextEndpoint, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
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
                && (p.DownloadSpeedTracker.CurrentBps.Kbps < minAcceptableSpeed || p.PeerChoking)
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
            await item.Value.DisposeAsync().ConfigureAwait(false);
        }
        await _requestManager.DisposeAsync().ConfigureAwait(false);
        await _uploadScheduler.DisposeAsync().ConfigureAwait(false);
        _semaphoreSlim.Dispose();
        DownloadInfo.Dispose();
    }
}
