using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.P2P.Messages;
using Netorrent.TorrentFile.FileStructure;
using ZLinq;

namespace Netorrent.P2P.Tcp;

internal class TcpPeersListeners(
    PeerId peerId,
    IReadOnlyList<TcpListener> tcpListeners,
    ILogger logger
) : IAsyncDisposable
{
    private readonly ConcurrentDictionary<InfoHash, PeersClient> _peersClientByInfoHash = new();
    private readonly Channel<TcpClient> _incomingConnections = Channel.CreateBounded<TcpClient>(
        new BoundedChannelOptions(128) { SingleWriter = false, SingleReader = true }
    );

    public int Port => ((IPEndPoint)tcpListeners[0].LocalEndpoint).Port;

    private CancellationTokenSource? _cancellationTokenSource;
    private Task? _runTask;
    private bool _disposed;

    public void Start()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _cancellationTokenSource = new();
        _runTask = _cancellationTokenSource.CancelOnFirstCompletionAndAwaitAllAsync([
            ListenAllAsync(_cancellationTokenSource.Token),
            ProcessIncomingConnectionsAsync(_cancellationTokenSource.Token),
        ]);
    }

    private async Task ListenAllAsync(CancellationToken cancellationToken)
    {
        var listenerTasks = tcpListeners.Select(i => ListenAsync(i, cancellationToken));
        await Task.WhenAny(listenerTasks).ConfigureAwait(false);
    }

    private async Task ProcessIncomingConnectionsAsync(CancellationToken cancellationToken)
    {
        await foreach (
            var tcpClient in _incomingConnections
                .Reader.ReadAllAsync(cancellationToken)
                .ConfigureAwait(false)
        )
        {
            var remoteEndPoint = (IPEndPoint)tcpClient.Client.RemoteEndPoint!;

            try
            {
                var handShake = await Handshake
                    .ReceiveHandshakeAsync(
                        tcpClient.GetStream(),
                        _peersClientByInfoHash.Keys,
                        peerId,
                        cancellationToken
                    )
                    .ConfigureAwait(false);

                if (
                    !_peersClientByInfoHash.TryGetValue(
                        handShake.InfoHash,
                        out var selectedPeersClient
                    )
                )
                {
                    tcpClient.Dispose();
                    continue;
                }

                await selectedPeersClient
                    .AddPeerAsync(
                        new TcpPeer(
                            tcpClient.GetMessageStream(handShake, selectedPeersClient.BitField),
                            (IPEndPoint)tcpClient.Client.RemoteEndPoint!,
                            peerId,
                            handShake.InfoHash,
                            selectedPeersClient.BitField
                        ),
                        cancellationToken
                    )
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (logger.IsEnabled(LogLevel.Debug))
                {
                    logger.LogDebug(
                        ex,
                        "Error while accepting incoming peer connection from {RemoteEndPoint}",
                        remoteEndPoint
                    );
                }
                tcpClient.Dispose();
            }
        }
    }

    private async Task ListenAsync(TcpListener tcpListener, CancellationToken cancellationToken)
    {
        tcpListener.Start();
        _cancellationTokenSource = new();

        while (!cancellationToken.IsCancellationRequested)
        {
            var tcpClient = await tcpListener
                .AcceptTcpClientAsync(cancellationToken)
                .ConfigureAwait(false);

            await _incomingConnections
                .Writer.WriteOrDisposeAsync(tcpClient, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    public void AddPeersClient(InfoHash infoHash, PeersClient peersClient)
    {
        _peersClientByInfoHash.TryAdd(infoHash, peersClient);
    }

    public void RemovePeersClient(InfoHash infoHash)
    {
        _peersClientByInfoHash.TryRemove(infoHash, out _);
    }

    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            _cancellationTokenSource?.Cancel();
            try
            {
                if (_runTask is not null)
                {
                    await _runTask.ConfigureAwait(false);
                }
            }
            catch { }

            foreach (var tcpListener in tcpListeners)
            {
                tcpListener.Dispose();
            }
        }
    }
}
