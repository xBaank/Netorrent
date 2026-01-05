using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.P2P.Download;
using Netorrent.P2P.Messages;
using Netorrent.P2P.Upload;
using R3;
using ZLinq;

namespace Netorrent.P2P;

internal class PeersClient(
    PeerId peerId,
    IRequestScheduler requestScheduler,
    IUploadScheduler uploadScheduler,
    IPiecePicker piecePicker,
    Bitfield bitField,
    ILogger logger
) : IAsyncDisposable
{
    const int MAX_ACTIVE_PEER_COUNT = 100;

    private readonly ConcurrentDictionary<PeerEndpoint, PeerConnection> _activePeers = [];
    private readonly ConcurrentQueue<PeerEndpoint> _knownPeers = [];
    private readonly List<Task> _peerTasks = [];
    private readonly Subject<PeerEndpoint> _peerConnected = new();
    private readonly Channel<PeerConnection> _peerConnections =
        Channel.CreateBounded<PeerConnection>(
            new BoundedChannelOptions(100) { SingleReader = true, SingleWriter = false }
        );

    public PeerId PeerId => peerId;
    public Observable<PeerEndpoint> PeerConnected => _peerConnected;
    public IReadOnlyDictionary<PeerEndpoint, PeerConnection> ActivePeers => _activePeers;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        await cts.CancelOnFirstCompletionAndAwaitAllAsync([
                requestScheduler.StartAsync(cts.Token),
                uploadScheduler.StartAsync(cts.Token),
                ProcessPeersAsync(cts.Token),
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
        await foreach (
            var peerConnection in _peerConnections
                .Reader.ReadAllAsync(cancellationToken)
                .ConfigureAwait(false)
        )
        {
            if (await CanConnectAsync(peerConnection).ConfigureAwait(false))
            {
                _peerTasks.Add(HandlePeerAsync(peerConnection, cancellationToken));
            }
        }
    }

    public async ValueTask AddPeerAsync(IPeer peer, CancellationToken cancellationToken)
    {
        var messageStream = await peer.ConnectAsync(cancellationToken).ConfigureAwait(false);
        var peerConnection = new PeerConnection(
            new(peer.PeerEndPoint, messageStream.Handshake.PeerId),
            bitField,
            uploadScheduler,
            requestScheduler,
            messageStream,
            new PeerRequestWindow(piecePicker.BlockSize),
            piecePicker
        );

        await _peerConnections
            .Writer.WriteAsync(peerConnection, cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task HandlePeerAsync(
        PeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        try
        {
            _peerConnected.OnNext(peerConnection.PeerEndpoint);
            await peerConnection.StartAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            await peerConnection.DisposeAsync().ConfigureAwait(false);
            return;
        }
        catch (Exception ex)
        {
            if (logger.IsEnabled(LogLevel.Debug))
            {
                logger.LogDebug(
                    ex,
                    "Exception on peer {peerId}",
                    peerConnection.PeerEndpoint.PeerId
                );
            }

            _activePeers.Remove(peerConnection.PeerEndpoint, out _);
            await peerConnection.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async ValueTask<bool> CanConnectAsync(PeerConnection peerConnection)
    {
        if (peerConnection.PeerEndpoint.PeerId == peerId)
        {
            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation(
                    "Ignored self connection to {EndPoint}",
                    peerConnection.PeerEndpoint
                );
            }

            await peerConnection.DisposeAsync().ConfigureAwait(false);
            return false;
        }

        if (
            _activePeers.ContainsKey(peerConnection.PeerEndpoint)
            || _activePeers
                .Keys.AsValueEnumerable()
                .Any(i => i.PeerId == peerConnection.PeerEndpoint.PeerId)
        )
        {
            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation(
                    "Ignored active peer from {EndPoint}",
                    peerConnection.PeerEndpoint
                );
            await peerConnection.DisposeAsync().ConfigureAwait(false);
            return false;
        }

        if (_activePeers.Count >= MAX_ACTIVE_PEER_COUNT)
        {
            _knownPeers.Enqueue(peerConnection.PeerEndpoint);
            await peerConnection.DisposeAsync().ConfigureAwait(false);
            return false;
        }

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("Connected to peer {PeerId}", peerConnection.PeerEndpoint.PeerId);

        _activePeers[peerConnection.PeerEndpoint] = peerConnection;
        return true;
    }

    public async ValueTask DisposeAsync()
    {
        var peersDisposeTasks = _activePeers.Values.Select(i => i.DisposeAsync().AsTask());
        await Task.WhenAll(peersDisposeTasks).ConfigureAwait(false);
        await requestScheduler.DisposeAsync().ConfigureAwait(false);
        await uploadScheduler.DisposeAsync().ConfigureAwait(false);
        _peerConnected.OnCompleted();
    }
}
