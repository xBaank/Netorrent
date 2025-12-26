using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.P2P.Messages;
using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.P2P.Tcp;

internal class TcpPeersListener(PeerId peerId, TcpListener tcpListener, ILogger logger)
    : IAsyncDisposable
{
    private readonly ConcurrentDictionary<InfoHash, PeersClient> _peersClientByInfoHash = new();

    public IPEndPoint EndPoint => (IPEndPoint)tcpListener.LocalEndpoint;

    private CancellationTokenSource? _cancellationTokenSource;
    private Task? _runTask;
    private bool _disposed;

    public void Start()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _runTask = StartAsync();
    }

    private async Task StartAsync()
    {
        tcpListener.Start();
        _cancellationTokenSource = new();

        while (!_cancellationTokenSource.Token.IsCancellationRequested)
        {
            var tcpClient = await tcpListener
                .AcceptTcpClientAsync(_cancellationTokenSource.Token)
                .ConfigureAwait(false);

            var remoteEndPoint = (IPEndPoint)tcpClient.Client.RemoteEndPoint!;

            try
            {
                var handShake = await Handshake
                    .ReceiveHandshakeAsync(
                        tcpClient.GetStream(),
                        _peersClientByInfoHash.Keys,
                        peerId,
                        _cancellationTokenSource.Token
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
                            tcpClient.GetMessageStream(handShake),
                            (IPEndPoint)tcpClient.Client.RemoteEndPoint!,
                            peerId,
                            handShake.InfoHash
                        ),
                        _cancellationTokenSource.Token
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
                    await _runTask.ConfigureAwait(false);
            }
            catch { }
            tcpListener.Dispose();
        }
    }
}
