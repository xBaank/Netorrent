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
    ConcurrentDictionary<PeerEndpoint, IPeerConnection> activePeers,
    PeerId peerId,
    IRequestScheduler requestScheduler,
    IUploadScheduler uploadScheduler,
    IPiecePicker piecePicker,
    Bitfield bitField,
    ILogger logger
) : IAsyncDisposable
{
    const int MAX_ACTIVE_PEER_COUNT = 100;

    private readonly ConcurrentQueue<PeerEndpoint> _knownPeers = [];
    private readonly Subject<PeerEndpoint> _peerConnected = new();
    private readonly Channel<PeerConnection> _peerConnections =
        Channel.CreateBounded<PeerConnection>(
            new BoundedChannelOptions(128) { SingleReader = true, SingleWriter = false }
        );

    public PeerId PeerId => peerId;
    public Observable<PeerEndpoint> PeerConnected => _peerConnected;
    public IReadOnlyDictionary<PeerEndpoint, IPeerConnection> ActivePeers => activePeers;

    public Bitfield BitField { get; } = bitField;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            await ProcessPeersAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            var disposeTasks = activePeers.Values.Select(i => i.DisposeAsync().AsTask());
            await Task.WhenAll(disposeTasks).ConfigureAwait(false);
            activePeers.Clear();
            throw;
        }
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
                _ = HandlePeerAsync(peerConnection, cancellationToken);
            }
        }
    }

    public async ValueTask AddPeerAsync(IPeer peer, CancellationToken cancellationToken)
    {
        var messageStream = await peer.ConnectAsync(cancellationToken).ConfigureAwait(false);
        var peerConnection = new PeerConnection(
            new(peer.PeerEndPoint, messageStream.Handshake.PeerId),
            BitField,
            uploadScheduler,
            requestScheduler,
            messageStream,
            new PeerRequestWindow(piecePicker.BlockSize),
            piecePicker
        );
        try
        {
            await _peerConnections
                .Writer.WriteAsync(peerConnection, cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            await peerConnection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
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
            activePeers.Remove(peerConnection.PeerEndpoint, out _);
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

            activePeers.Remove(peerConnection.PeerEndpoint, out _);
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
            activePeers.ContainsKey(peerConnection.PeerEndpoint)
            || activePeers
                .Keys.AsValueEnumerable()
                .Any(i => i.PeerId == peerConnection.PeerEndpoint.PeerId)
        )
        {
            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation(
                    "Ignored active peer from {EndPoint}",
                    peerConnection.PeerEndpoint
                );
            }

            await peerConnection.DisposeAsync().ConfigureAwait(false);
            return false;
        }

        if (activePeers.Count >= MAX_ACTIVE_PEER_COUNT)
        {
            _knownPeers.Enqueue(peerConnection.PeerEndpoint);
            await peerConnection.DisposeAsync().ConfigureAwait(false);
            return false;
        }

        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("Connected to peer {PeerId}", peerConnection.PeerEndpoint.PeerId);
        }

        activePeers[peerConnection.PeerEndpoint] = peerConnection;
        return true;
    }

    public async ValueTask DisposeAsync()
    {
        var peersDisposeTasks = activePeers.Values.Select(i => i.DisposeAsync().AsTask());
        await Task.WhenAll(peersDisposeTasks).ConfigureAwait(false);
        _peerConnected.OnCompleted();
    }
}
