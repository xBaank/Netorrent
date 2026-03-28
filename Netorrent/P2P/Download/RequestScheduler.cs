using Microsoft.Extensions.Logging;
using Netorrent.ActorSystem;
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
) : Actor<DownloadMessage>, IRequestScheduler
{
    const int MinPeersForRarity = 6;

    private static readonly DownloadMessage.CheckTimeoutMessage _timeoutMessage = new();
    private readonly Dictionary<int, PieceBuffer> _pieceBuffers = [];

    protected override async Task OnStartedAsync(CancellationToken cancellationToken)
    {
        ScheduleRepeatedly(TimeSpan.Zero, 1.Seconds, _timeoutMessage);
        await WarmupAsync(cancellationToken).ConfigureAwait(false);
    }

    protected override async ValueTask OnReceiveAsync(
        DownloadMessage message,
        CancellationToken cancellationToken
    )
    {
        switch (message)
        {
            case DownloadMessage.BlockMessage blockMessage:
                await ProcessBlockAsync(blockMessage.Block, cancellationToken)
                    .ConfigureAwait(false);
                break;
            case DownloadMessage.CheckTimeoutMessage:
                CheckTimeout();
                break;
            case DownloadMessage.ScheduleMessage scheduleMessage:
                ScheduleRequests(scheduleMessage.PeerConnection);
                if (piecePicker.IsEndGame)
                {
                    ScheduleRequests();
                }
                break;
        }
    }

    protected override async ValueTask DrainAsync()
    {
        await foreach (var item in MailboxReader.ReadAllAsync().ConfigureAwait(false))
        {
            if (item is DownloadMessage.BlockMessage blockMessage)
            {
                blockMessage.Dispose();
            }
        }

        foreach (var (_, item) in _pieceBuffers)
        {
            item.Dispose();
        }
        _pieceBuffers.Clear();
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
                Tell(new DownloadMessage.ScheduleMessage(freePeer));
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
            Tell(new DownloadMessage.ScheduleMessage(peerConnection));
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
        var message = new DownloadMessage.BlockMessage(block);
        try
        {
            await SendAsync(message, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            message.Dispose();
            throw;
        }
    }
}
