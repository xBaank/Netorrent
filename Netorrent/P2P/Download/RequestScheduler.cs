using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Messages;
using Netorrent.Statistics;
using ZLinq;

namespace Netorrent.P2P.Download;

internal class RequestScheduler(
    IReadOnlyDictionary<PeerEndpoint, IPeerConnection> peers,
    IPiecePicker piecePicker,
    Bitfield myBitfield,
    DataStatistics data,
    TimeSpan warmupTime,
    TimeSpan timeoutTime,
    IPieceStorage pieceStorage,
    ILogger logger
) : IRequestScheduler
{
    const int MinPeersForRarity = 6;

    private readonly Channel<DownloadMessage> _downloadMessageChannel =
        Channel.CreateBounded<DownloadMessage>(
            new BoundedChannelOptions(128) { SingleWriter = false, SingleReader = true }
        );

    private static readonly DownloadMessage.CheckTimeoutMessage _timeoutMessage = new();
    private readonly Dictionary<int, PieceBuffer> _pieceBuffers = [];
    private CancellationTokenSource? _cts;
    private Task? _runningTask;
    private bool _disposed;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _runningTask = _cts.CancelOnFirstCompletionAndAwaitAllAsync([
            ScheduleTimeoutsBlocksAsync(_cts.Token),
            ProcessDownloadMessagesAsync(_cts.Token),
        ]);
        return _runningTask;
    }

    private async Task ScheduleTimeoutsBlocksAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            await _downloadMessageChannel
                .Writer.WriteAsync(_timeoutMessage, cancellationToken)
                .ConfigureAwait(false);
            await Task.Delay(1.Seconds, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task ProcessDownloadMessagesAsync(CancellationToken cancellationToken)
    {
        await WarmupAsync(cancellationToken).ConfigureAwait(false);
        await foreach (
            var downloadMessage in _downloadMessageChannel
                .Reader.ReadAllAsync(cancellationToken)
                .ConfigureAwait(false)
        )
        {
            if (downloadMessage is DownloadMessage.BlockMessage blockMessage)
            {
                await ProcessBlockAsync(blockMessage.Block, cancellationToken)
                    .ConfigureAwait(false);
                continue;
            }

            if (downloadMessage is DownloadMessage.CheckTimeoutMessage)
            {
                CheckTimeout();
                continue;
            }

            if (downloadMessage is DownloadMessage.ScheduleMessage scheduleMessage)
            {
                ScheduleRequests(scheduleMessage.PeerConnection);
                if (piecePicker.IsEndGame)
                {
                    ScheduleRequests();
                }
                continue;
            }
        }
    }

    private async ValueTask ProcessBlockAsync(Block block, CancellationToken cancellationToken)
    {
        //TODO penalize?
        if (!piecePicker.TryGetRequestedBlock(block, out var requestedBlock))
        {
            block.Dispose();
            return;
        }

        if (!_pieceBuffers.TryGetValue(block.Index, out var pieceBuffer))
        {
            pieceBuffer = new PieceBuffer(block.Index, pieceStorage, piecePicker);
            _pieceBuffers[block.Index] = pieceBuffer;
        }

        foreach (var peerConnection in requestedBlock.RequestedFrom)
        {
            if (peerConnection != block.FromPeer)
            {
                peerConnection.TrySendCancel(requestedBlock);
            }
            peerConnection.DecrementRequestedBlock();
        }

        block.FromPeer.PeerRequestWindow.ReceivedBlock(block.FromPeer.DownloadTracker.Speed.Bps);
        await pieceBuffer.AddBlockAsync(block, cancellationToken).ConfigureAwait(false);
        requestedBlock.State = RequestBlockState.Completed;
        requestedBlock.TimeoutAt = null;
        requestedBlock.RequestedFrom.Clear();
        TryRequest(block.FromPeer);

        if (!pieceBuffer.IsComplete)
        {
            return;
        }

        var isWritten = false;
        try
        {
            isWritten = pieceBuffer.VerifyPiece();
        }
        finally
        {
            piecePicker.CompletePiece(block.Index);
            if (_pieceBuffers.Remove(block.Index, out var removedBuffer))
            {
                removedBuffer.Dispose();
            }
        }

        if (isWritten)
        {
            if (!myBitfield.HasPiece(block.Index))
            {
                data.AddVerifiedBytes(pieceBuffer.Size);
                myBitfield.SetPiece(block.Index);
            }
        }
        else
        {
            // Retry with a fresh buffer
            _pieceBuffers[block.Index] = new PieceBuffer(block.Index, pieceStorage, piecePicker);
            data.AddDiscardedBytes(pieceBuffer.Size);
        }
    }

    private void CheckTimeout()
    {
        var currentPeers = peers.AsValueEnumerable().Select(i => i.Value);
        foreach (var requestBlock in piecePicker.GetTimeoutRequestBlocks())
        {
            requestBlock.State = RequestBlockState.Pending;
            requestBlock.TimeoutAt = null; //TODO set ALL request blocks by lastRequestedFrom requester to pending as they are all more likely to be timed out
            var freePeer = currentPeers
                .Where(i => !i.PeerChoking.CurrentValue)
                .Where(i => i.AmInterested.CurrentValue)
                .Where(i => !requestBlock.RequestedFrom.Contains(i))
                .FirstOrDefault();

            if (freePeer is not null)
            {
                _downloadMessageChannel.Writer.TryWrite(
                    new DownloadMessage.ScheduleMessage(freePeer)
                );
            }
        }
    }

    private async Task WarmupAsync(CancellationToken cancellationToken)
    {
        var warmupDeadline = DateTime.UtcNow + warmupTime;
        while (DateTime.UtcNow < warmupDeadline && !cancellationToken.IsCancellationRequested)
        {
            var minPeersReady = peers
                .AsValueEnumerable()
                .Select(i => i.Value)
                .Count(i => i.AmInterested.CurrentValue && !i.PeerChoking.CurrentValue);

            if (minPeersReady >= MinPeersForRarity)
            {
                return;
            }

            await Task.Delay(100, cancellationToken).ConfigureAwait(false);
        }
    }

    private void ScheduleRequests()
    {
        var freePeers = peers
            .AsValueEnumerable()
            .Select(i => i.Value)
            .Where(i => !i.PeerChoking.CurrentValue)
            .Where(i => i.AmInterested.CurrentValue);

        foreach (var peer in freePeers)
        {
            ScheduleRequests(peer);
        }
    }

    private void ScheduleRequests(IPeerConnection peerConnection)
    {
        while (
            peerConnection.RequestedBlocksCount
            < peerConnection.PeerRequestWindow.MaxInFlightRequests
        )
        {
            if (!piecePicker.TryGetRequestBlock(peerConnection, out var requestBlock))
            {
                return;
            }

            if (peerConnection.TrySendRequest(requestBlock))
            {
                requestBlock.State = RequestBlockState.Requested;
                requestBlock.TimeoutAt = CalculateTimeout(peerConnection, requestBlock.Length);
                requestBlock.RequestedFrom.Add(peerConnection);
                peerConnection.IncrementRequestedBlock();
            }
            else
            {
                if (logger.IsEnabled(LogLevel.Information))
                {
                    logger.LogInformation(
                        "Failed to send request block {Index}:{Begin} to peer {Peer} with {blocks}/{maxblocks}",
                        requestBlock.Index,
                        requestBlock.Begin,
                        peerConnection.PeerEndpoint.PeerId,
                        peerConnection.RequestedBlocksCount,
                        peerConnection.PeerRequestWindow.MaxInFlightRequests
                    );
                }
                return;
            }
        }
    }

    public void TryRequest(IPeerConnection peerConnection)
    {
        if (!peerConnection.PeerChoking.CurrentValue && peerConnection.AmInterested.CurrentValue)
        {
            _downloadMessageChannel.Writer.TryWrite(
                new DownloadMessage.ScheduleMessage(peerConnection)
            );
        }
    }

    private DateTimeOffset CalculateTimeout(IPeerConnection peerConnection, int blockLength)
    {
        var speedBps = peerConnection.DownloadTracker.Speed.Bps;
        if (speedBps <= 0)
        {
            return DateTimeOffset.UtcNow + timeoutTime;
        }

        var estimatedSeconds = blockLength / speedBps;
        var timeoutSeconds = estimatedSeconds * 3 + 2;
        return DateTimeOffset.UtcNow + Math.Min(timeoutSeconds, 60).Seconds;
    }

    public async ValueTask ReceiveBlockAsync(Block block, CancellationToken cancellationToken)
    {
        try
        {
            await _downloadMessageChannel
                .Writer.WriteAsync(new DownloadMessage.BlockMessage(block), cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            block.Dispose();
            throw;
        }
    }

    private async ValueTask DrainChannelsAsync()
    {
        await foreach (
            var item in _downloadMessageChannel.Reader.ReadAllAsync().ConfigureAwait(false)
        )
        {
            if (item is DownloadMessage.BlockMessage blockMessage)
            {
                blockMessage.Block.Dispose();
            }
        }

        foreach (var (_, item) in _pieceBuffers)
        {
            item.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            _cts?.Cancel();
            _downloadMessageChannel.Writer.TryComplete();

            try
            {
                if (_runningTask is not null)
                {
                    await _runningTask.ConfigureAwait(false);
                }
            }
            catch { }

            await DrainChannelsAsync().ConfigureAwait(false);
            _cts?.Dispose();
        }
    }
}
