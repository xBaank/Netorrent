using System.Collections.Concurrent;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Download;
using Netorrent.P2P.Messages;
using Netorrent.P2P.Upload;
using R3;
using ZLinq;

namespace Netorrent.P2P;

internal class PeersClient(
    ReadOnlyMemory<byte> infoHash,
    PeerId peerId,
    IRequestScheduler requestScheduler,
    IUploadScheduler uploadScheduler,
    IPiecePicker piecePicker,
    Bitfield bitField,
    ChannelReader<IPEndPoint> trackersChannel,
    ILogger logger,
    Func<IPAddress, IPAddress>? peerIpProxy
) : IAsyncDisposable
{
    const int MAX_ACTIVE_PEER_COUNT = 50;

    private readonly ConcurrentDictionary<PeerEndpoint, PeerConnection> _activePeers = [];
    private readonly ConcurrentQueue<PeerEndpoint> _knownPeers = [];
    private readonly SemaphoreSlim _semaphoreSlim = new(1);
    private readonly List<Task> _peerTasks = [];
    private readonly Subject<PeerEndpoint> _peerConnected = new();

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
        List<Task> connectTasks = new(100);

        await foreach (
            var iPEndPoint in trackersChannel.ReadAllAsync(cancellationToken).ConfigureAwait(false)
        )
        {
            var targetEndPoint = new IPEndPoint(
                peerIpProxy?.Invoke(iPEndPoint.Address) ?? iPEndPoint.Address,
                iPEndPoint.Port
            );
            try
            {
                var messageStream = await GetTcpMessageStreamAsync(iPEndPoint, cancellationToken)
                    .ConfigureAwait(false);
                connectTasks.Add(AddPeerAsync(messageStream, iPEndPoint, cancellationToken));
            }
            catch (Exception ex)
            {
                if (logger.IsEnabled(LogLevel.Debug))
                {
                    logger.LogDebug(ex, "Error connecting to {ip}", targetEndPoint);
                }
            }

            if (connectTasks.Count >= 100)
            {
                await Task.WhenAll(connectTasks).ConfigureAwait(false);
            }
        }

        await Task.WhenAll(connectTasks).ConfigureAwait(false);
    }

    private async ValueTask<TcpMessageStream> GetTcpMessageStreamAsync(
        IPEndPoint iPEndPoint,
        CancellationToken cancellationToken
    )
    {
        using var cts = cancellationToken.WithTimeout(10.Seconds);
        var tcpClient = new TcpClient();
        await tcpClient.ConnectAsync(iPEndPoint, cts.Token).ConfigureAwait(false);

        var handshake = await tcpClient
            .GetStream()
            .PerformHandshakeAsync(infoHash, peerId, cancellationToken)
            .ConfigureAwait(false);

        return tcpClient.GetMessageStream(peerId);
    }

    public async Task AddPeerAsync(
        IMessageStream messageStream,
        IPEndPoint iPEndPoint,
        CancellationToken cancellationToken = default
    )
    {
        var peerConnection = new PeerConnection(
            new(iPEndPoint, messageStream.PeerId),
            bitField,
            uploadScheduler,
            requestScheduler,
            messageStream,
            new PeerRequestWindow(piecePicker.BlockSize),
            piecePicker
        );

        await _semaphoreSlim.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
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
                return;
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
                return;
            }

            if (_activePeers.Count >= MAX_ACTIVE_PEER_COUNT)
            {
                _knownPeers.Enqueue(peerConnection.PeerEndpoint);
                await peerConnection.DisposeAsync().ConfigureAwait(false);
                return;
            }

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation(
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

            await TryAddNextPeerAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask TryAddNextPeerAsync(CancellationToken cancellationToken)
    {
        var hasError = false;
        var hasEndpoint = _knownPeers.TryDequeue(out var nextEndpoint);

        if (!hasEndpoint)
        {
            return;
        }

        do
        {
            try
            {
                var messageStream = await GetTcpMessageStreamAsync(
                        nextEndpoint.EndPoint,
                        cancellationToken
                    )
                    .ConfigureAwait(false);

                await AddPeerAsync(messageStream, nextEndpoint.EndPoint, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                hasError = true;
                if (logger.IsEnabled(LogLevel.Debug))
                {
                    logger.LogDebug(ex, "Error connecting to {ip}", nextEndpoint.EndPoint);
                }
                continue;
            }
        } while (hasError && _knownPeers.TryDequeue(out nextEndpoint));
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var item in _activePeers)
        {
            await item.Value.DisposeAsync().ConfigureAwait(false);
        }
        await requestScheduler.DisposeAsync().ConfigureAwait(false);
        await uploadScheduler.DisposeAsync().ConfigureAwait(false);
        _semaphoreSlim.Dispose();
        _peerConnected.OnCompleted();
    }
}
