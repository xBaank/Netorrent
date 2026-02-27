using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Messages;
using Netorrent.Statistics;
using R3;
using ZLinq;

namespace Netorrent.P2P.Upload;

//https://www.bittorrent.org/beps/bep_0003.html
//https://read.seas.harvard.edu/~kohler/pubs/legout07clustering.pdf Section 2.3
internal class UploadScheduler(
    IReadOnlyDictionary<PeerEndpoint, IPeerConnection> peers,
    IPieceStorage pieceStorage,
    Bitfield bitfield,
    DataStatistics data,
    ILogger logger
) : IUploadScheduler
{
    const int MaxActivePeers = 4; //TODO Add an option for this to be changed or rate based

    private readonly Channel<UploadMessage> _uploadMessagesChannel =
        Channel.CreateBounded<UploadMessage>(
            new BoundedChannelOptions(128) { SingleWriter = false, SingleReader = true }
        );
    private readonly Lock _cancelLock = new();
    private readonly HashSet<RequestBlock> _requests = [];
    private static readonly UploadMessage.CheckRoundMessage _checkRoundMessage = new();
    private readonly TimeSpan _interval = 10.Seconds;

    private byte _round = 1;
    private CancellationTokenSource? _cts;
    private Task? _runningTask;
    private bool _disposed;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _runningTask = Task.RunUntilFirstCompletesAsync(
            [ScheduleRoundsAsync, ProcessUploadMessagesAsync],
            _cts
        );
        return _runningTask;
    }

    public async Task ScheduleRoundsAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            await _uploadMessagesChannel
                .Writer.WriteAsync(_checkRoundMessage, cancellationToken)
                .ConfigureAwait(false);
            await Task.Delay(_interval, cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task ProcessUploadMessagesAsync(CancellationToken cancellationToken)
    {
        await foreach (
            var uploadMessage in _uploadMessagesChannel.Reader.ReadAllAsync(cancellationToken)
        )
        {
            if (uploadMessage is UploadMessage.CheckRoundMessage)
            {
                RunRound();
                continue;
            }

            if (uploadMessage is UploadMessage.RequestBlockMessage requestBlockMessage)
            {
                await ProcessRequestAsync(requestBlockMessage.RequestBlock, cancellationToken)
                    .ConfigureAwait(false);
                continue;
            }
        }
    }

    private void RunRound()
    {
        try
        {
            RegularChoke();
            _round++;

            if (_round == 3)
            {
                OptimisticChoke();
                _round = 1;
            }
        }
        catch (Exception ex)
        {
            if (logger.IsEnabled(LogLevel.Error))
            {
                logger.LogError(ex, "Error running round {round}", _round);
            }
        }
    }

    private async ValueTask ProcessRequestAsync(
        RequestBlock requestBlock,
        CancellationToken cancellationToken
    )
    {
        var peer = requestBlock.RequestedFrom[0];

        lock (_cancelLock)
        {
            if (
                _requests.TryGetValue(requestBlock, out var actualValue)
                && actualValue.State == RequestBlockState.Cancelled
            )
            {
                _requests.Remove(actualValue);
                peer.DecrementUploadRequested();
                return;
            }
        }

        var pieceData = await pieceStorage
            .ReadAsync(
                requestBlock.Index,
                requestBlock.Begin,
                requestBlock.Length,
                cancellationToken
            )
            .ConfigureAwait(false);

        var block = new Block(requestBlock.Index, requestBlock.Begin, pieceData, peer);
        try
        {
            if (peer.TrySendBlock(block))
            {
                data.AddUploadedBytes(block.Payload.Length); //TODO Move this to message stream after data if flushed ?
            }
            else
            {
                block.Dispose();
            }
        }
        catch (Exception ex)
        {
            block.Dispose();
            if (logger.IsEnabled(LogLevel.Error))
            {
                logger.LogError(
                    ex,
                    "Failed to send block Index: {Index}, Begin: {Begin}, Length: {Length} to Peer: {PeerEndPoint}",
                    requestBlock.Index,
                    requestBlock.Begin,
                    requestBlock.Length,
                    peer
                );
            }
        }
        finally
        {
            lock (_cancelLock)
            {
                _requests.Remove(requestBlock);
            }
            peer.DecrementUploadRequested();
        }
    }

    private void OptimisticChoke()
    {
        var peerToUnchoke = peers
            .AsValueEnumerable()
            .Select(i => i.Value)
            .Where(i => i.AmChoking.CurrentValue)
            .Where(i => i.PeerInterested.CurrentValue)
            .Shuffle()
            .FirstOrDefault();

        var activePeers = peers
            .AsValueEnumerable()
            .Select(i => i.Value)
            .Where(i => i.ActiveDownloader.CurrentValue)
            .Count();

        if (peerToUnchoke is null)
        {
            return;
        }

        if (activePeers == MaxActivePeers)
        {
            var worstPeer = peers
                .Select(i => i.Value)
                .AsValueEnumerable()
                .OrderBy(i =>
                    i.MyBitField.IsComplete
                        ? i.UploadTracker.Speed.Bps
                        : i.DownloadTracker.Speed.Bps
                )
                .FirstOrDefault();

            worstPeer?.ActiveDownloader.Value = false;
            worstPeer?.Choke();
        }

        peerToUnchoke.ActiveDownloader.Value = true;
        peerToUnchoke.Unchoke();
    }

    //TODO implement new choke algorithm as seeder to avoid free riders
    private void RegularChoke()
    {
        var bestPeersByDownload = peers
            .Select(i => i.Value)
            .AsValueEnumerable()
            .OrderByDescending(i =>
                i.MyBitField.IsComplete ? i.UploadTracker.Speed.Bps : i.DownloadTracker.Speed.Bps
            )
            .ToArray();

        var toUnchokeCount = MaxActivePeers - 1;
        var activePeersCount = 0;

        foreach (var peer in bestPeersByDownload)
        {
            //Choke and ignore peers that were not active in the last 30 seconds
            if (peer.TimeSinceReceivedBlock >= 30.Seconds)
            {
                peer.ActiveDownloader.Value = false;
                peer.Choke();
                continue;
            }

            //If the peer is already as active downloader we do nothing
            if (peer.ActiveDownloader.CurrentValue)
            {
                activePeersCount++;
                continue;
            }

            //Peer can be unchoked
            if (activePeersCount < toUnchokeCount)
            {
                peer.Unchoke();
            }
            //Peer can't be added so it's choked and removed if it's an active downloader
            else
            {
                peer.Choke();
                peer.ActiveDownloader.Value = false;
            }

            //Peer is interested and unchoked (we add it)
            if (!peer.AmChoking.CurrentValue && peer.PeerInterested.CurrentValue)
            {
                peer.ActiveDownloader.Value = true;
                activePeersCount++;
            }
        }
    }

    public void TryRunRound(IPeerConnection peerConnection)
    {
        if (peerConnection.PeerInterested.CurrentValue && !peerConnection.AmChoking.CurrentValue)
        {
            _uploadMessagesChannel.Writer.TryWrite(_checkRoundMessage);
        }
    }

    public async ValueTask AddRequestAsync(
        RequestBlock request,
        CancellationToken cancellationToken
    )
    {
        var from = request.RequestedFrom[0];

        if (!bitfield.HasPiece(request.Index))
        {
            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation(
                    "Peer {peer} requested a block we don't have",
                    from.PeerEndpoint.PeerId
                );
            }

            return;
        }

        //TODO penalize?
        if (
            !peers.TryGetValue(from.PeerEndpoint, out var peer)
            || !peer.ActiveDownloader.CurrentValue
        )
        {
            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation("Peer {peer} is not unchoked", from.PeerEndpoint.PeerId);
            }

            return;
        }

        bool shouldAdd;

        lock (_cancelLock)
        {
            shouldAdd = _requests.Add(request);
        }

        if (shouldAdd)
        {
            from.IncrementUploadRequested();

            await _uploadMessagesChannel
                .Writer.WriteAsync(
                    new UploadMessage.RequestBlockMessage(request),
                    cancellationToken
                )
                .ConfigureAwait(false);
        }
    }

    public void CancelRequest(RequestBlock cancelled)
    {
        lock (_cancelLock)
        {
            if (_requests.TryGetValue(cancelled, out var requestBlock))
            {
                cancelled.State = RequestBlockState.Cancelled;
            }
        }
    }

    private async ValueTask DrainChannelsAsync()
    {
        await foreach (var _ in _uploadMessagesChannel.Reader.ReadAllAsync().ConfigureAwait(false))
        { }
        _requests.Clear();
    }

    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            _cts?.Cancel();
            _uploadMessagesChannel.Writer.TryComplete();

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
