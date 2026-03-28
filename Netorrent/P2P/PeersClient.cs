using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Netorrent.ActorSystem;
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

    private readonly Actor<PeerConnection> _actor = new();
    private readonly ConcurrentQueue<PeerEndpoint> _knownPeers = [];
    private readonly Subject<PeerEndpoint> _peerConnected = new();
    private readonly List<Task> _peerTasks = [];

    public PeerId PeerId => peerId;
    public Observable<PeerEndpoint> PeerConnected => _peerConnected;
    public IReadOnlyDictionary<PeerEndpoint, IPeerConnection> ActivePeers => activePeers;

    public Bitfield BitField { get; } = bitField;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            await _actor.StartAsync(OnReceiveAsync, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            try
            {
                await Task.WhenAll(_peerTasks).ConfigureAwait(false);
            }
            catch { }

            var disposeTasks = activePeers.Values.Select(i => i.DisposeAsync().AsTask());
            await Task.WhenAll(disposeTasks).ConfigureAwait(false);
            activePeers.Clear();
            _peerConnected.OnCompleted();
        }
    }

    private async ValueTask OnReceiveAsync(
        PeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        _peerTasks.RemoveAll(t => t.IsCompleted);

        if (await CanConnectAsync(peerConnection).ConfigureAwait(false))
        {
            _peerConnected.OnNext(peerConnection.PeerEndpoint);
            _peerTasks.Add(HandlePeerAsync(peerConnection, cancellationToken));
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
            await _actor.SendAsync(peerConnection, cancellationToken).ConfigureAwait(false);
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
            await peerConnection.StartAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            if (logger.IsEnabled(LogLevel.Debug))
            {
                logger.LogDebug(
                    ex,
                    "Exception on peer {peerId}",
                    peerConnection.PeerEndpoint.PeerId
                );
            }
        }
        finally
        {
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
        await _actor.DisposeAsync().ConfigureAwait(false);

        await foreach (var connection in _actor.MailboxReader.ReadAllAsync().ConfigureAwait(false))
        {
            await connection.DisposeAsync().ConfigureAwait(false);
        }
    }
}
