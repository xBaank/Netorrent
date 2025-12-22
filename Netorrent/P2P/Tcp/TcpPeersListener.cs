using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.Other;
using Netorrent.P2P.Messages;

namespace Netorrent.P2P.Tcp;

internal class TcpPeersListener(PeerId peerId, ILogger logger) : IAsyncDisposable
{
    private readonly TcpListener _tcpListener = TcpListener.GetFreeTcpListener();
    private readonly ConcurrentDictionary<
        ReadOnlyMemory<byte>,
        PeersClient
    > _peersClientByInfoHash = new(ReadOnlyMemoryEqualityComparer<byte>.Instance);

    public IPEndPoint EndPoint => (IPEndPoint)_tcpListener.LocalEndpoint;

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
        _tcpListener.Start();
        _cancellationTokenSource = new();

        while (!_cancellationTokenSource.Token.IsCancellationRequested)
        {
            var tcpClient = await _tcpListener
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
                        new TcpReceivedPeer(tcpClient, remoteEndPoint, peerId, handShake),
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

    public void AddPeersClient(ReadOnlyMemory<byte> infoHash, PeersClient peersClient)
    {
        _peersClientByInfoHash.TryAdd(infoHash, peersClient);
    }

    public void RemovePeersClient(ReadOnlyMemory<byte> infoHash)
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
            _tcpListener.Dispose();
        }
    }
}
