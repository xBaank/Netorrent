using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Download;
using Netorrent.P2P.Messages;
using Netorrent.P2P.Upload;
using ZLinq;

namespace Netorrent.P2P;

internal class P2PClient : IAsyncDisposable
{
    const int MAX_ACTIVE_PEER_COUNT = 50;
    const int PEER_TIMEOUT_SECONDS = 120;

    private readonly TcpListener _listener = TcpListener.GetFreeTcpListener();
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<PeerEndpoint, PeerConnection> _activePeers = [];
    private readonly ConcurrentQueue<IPEndPoint> _knownPeers = [];
    private readonly ReadOnlyMemory<byte> _infoHash;
    private readonly PeerId _peerId;
    private readonly Bitfield _bitField;
    private readonly IRequestScheduler _requestScheduler;
    private readonly IUploadScheduler _uploadScheduler;
    private readonly int _blockSize;
    private readonly ChannelReader<IPEndPoint> _trackersChannel;
    private readonly Func<IPAddress, IPAddress>? _peerIpProxy;
    private readonly SemaphoreSlim _semaphoreSlim = new(1);
    private readonly List<Task> _peerTasks = [];
    public IReadOnlyDictionary<PeerEndpoint, PeerConnection> ActivePeers => _activePeers;
    public IPEndPoint EndPoint => (IPEndPoint)_listener.LocalEndpoint;

    public P2PClient(
        ReadOnlyMemory<byte> infoHash,
        PeerId peerId,
        IRequestScheduler requestScheduler,
        IUploadScheduler uploadScheduler,
        int blockSize,
        Bitfield bitField,
        ChannelReader<IPEndPoint> trackersChannel,
        ILogger logger,
        Func<IPAddress, IPAddress>? peerIpProxy
    )
    {
        _peerId = peerId;
        _bitField = bitField;
        _infoHash = infoHash;
        _trackersChannel = trackersChannel;
        _logger = logger;
        _peerIpProxy = peerIpProxy;
        _blockSize = blockSize;
        _requestScheduler = requestScheduler;
        _uploadScheduler = uploadScheduler;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        await cts.CancelOnFirstCompletionAndAwaitAllAsync([
                _requestScheduler.StartAsync(cts.Token),
                _uploadScheduler.StartAsync(cts.Token),
                ProcessPeersAsync(cts.Token),
                ListenToPeersAsync(cts.Token),
            ])
            .ConfigureAwait(false);

        try
        {
            await Task.WhenAll(_peerTasks).ConfigureAwait(false);
        }
        catch { }
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

        var messageStream = new MessageStream(client!.GetStream(), PEER_TIMEOUT_SECONDS.Seconds);
        PeerConnection peerConnection;

        try
        {
            peerConnection = await PeerConnection.CreatePeerConnectionAsync(
                _bitField,
                _uploadScheduler,
                _requestScheduler,
                messageStream,
                new(_blockSize),
                amInitiating,
                _infoHash,
                _peerId,
                iPEndPoint,
                cancellationToken
            );
        }
        catch (Exception ex)
        {
            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug(ex, "Error handshaking to {ip}", iPEndPoint);
            }

            return;
        }

        await _semaphoreSlim.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (peerConnection.PeerEndpoint.PeerId == _peerId)
            {
                if (_logger.IsEnabled(LogLevel.Information))
                    _logger.LogInformation("Ignored self connection to {EndPoint}", iPEndPoint);
                await peerConnection.DisposeAsync().ConfigureAwait(false);
                return;
            }

            if (
                _activePeers.ContainsKey(peerConnection.PeerEndpoint)
                || _activePeers
                    .Keys.AsValueEnumerable()
                    .Any(i => i.PeerId == peerConnection.PeerEndpoint.PeerId)
            )
            {
                if (_logger.IsEnabled(LogLevel.Information))
                    _logger.LogInformation("Ignored active peer from {EndPoint}", iPEndPoint);
                await peerConnection.DisposeAsync().ConfigureAwait(false);
                return;
            }

            if (_activePeers.Count >= MAX_ACTIVE_PEER_COUNT)
            {
                _knownPeers.Enqueue(iPEndPoint);
                return;
            }

            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation(
                    "Connected to peer {PeerId}",
                    peerConnection.PeerEndpoint.PeerId
                );

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
                _logger.LogDebug(
                    ex,
                    "Exception on peer {peerId}",
                    peerConnection.PeerEndpoint.PeerId
                );
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

    public async ValueTask DisposeAsync()
    {
        _listener.Stop();
        foreach (var item in _activePeers)
        {
            await item.Value.DisposeAsync().ConfigureAwait(false);
        }
        await _requestScheduler.DisposeAsync().ConfigureAwait(false);
        await _uploadScheduler.DisposeAsync().ConfigureAwait(false);
        _semaphoreSlim.Dispose();
    }
}
