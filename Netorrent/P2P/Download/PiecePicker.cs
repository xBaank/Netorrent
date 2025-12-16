using System.Collections.Concurrent;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Messages;
using Netorrent.Statistics;
using ZLinq;

namespace Netorrent.P2P.Download;

internal class PiecePicker(
    Bitfield myBitfield,
    FileManager fileManager,
    TransferStatistics transfer
) : IAsyncDisposable
{
    public const int TimeoutSeconds = 10;

    private readonly int[] _pieceRarity = new int[myBitfield.Length];
    private readonly Lock _requestBlocksLock = new();
    private readonly ConcurrentDictionary<int, RequestBlock[]> _requestBlocks = [];
    private readonly ConcurrentDictionary<int, PieceBuffer> _pieceBuffers = [];

    public Bitfield Bitfield => myBitfield;

    public void IncreaseRarity(int index)
    {
        Interlocked.Increment(ref _pieceRarity[index]);
    }

    public void DecreaseRarity(int index)
    {
        Interlocked.Decrement(ref _pieceRarity[index]);
    }

    public async ValueTask ReceiveBlockAsync(
        Block receiveBlock,
        CancellationToken cancellationToken
    )
    {
        if (!_pieceBuffers.TryGetValue(receiveBlock.Index, out var pieceBuffer))
        {
            pieceBuffer = new PieceBuffer(receiveBlock.Index, fileManager);
            _pieceBuffers[receiveBlock.Index] = pieceBuffer;
        }

        if (!_requestBlocks.TryGetValue(receiveBlock.Index, out var requestBlocks))
        {
            return;
        }

        lock (_requestBlocksLock)
        {
            RequestBlock? requestedBlock = null;
            foreach (var requestBlock in requestBlocks)
            {
                if (
                    requestBlock.State != RequestBlockState.Completed
                    && requestBlock.Index == receiveBlock.Index
                    && requestBlock.Begin == receiveBlock.Begin
                    && requestBlock.Length == receiveBlock.Payload.Length
                    && requestBlock.RequestedFrom.Contains(receiveBlock.FromPeer)
                )
                {
                    requestedBlock = requestBlock;
                    break;
                }
            }

            if (requestedBlock is not null)
            {
                var rtt = requestedBlock.RequestedAt.HasValue
                    ? receiveBlock.ReceivedAt - requestedBlock.RequestedAt.Value
                    : TimeoutSeconds.Seconds;
                receiveBlock.FromPeer.PeerRequestWindow.CalculateWindow(
                    (long)receiveBlock.FromPeer.DownloadSpeedTracker.CurrentBps.Bps,
                    rtt
                );
                receiveBlock.FromPeer.DecrementRequestedBlock();
                requestedBlock.State = RequestBlockState.Completed;
                requestedBlock.RequestedAt = null;
                requestedBlock.RequestedFrom.Clear();
                pieceBuffer.AddBlock(receiveBlock);
                transfer.AddDownloadedBytes(receiveBlock.Payload.Length);
            }
        }

        if (!pieceBuffer.IsComplete)
            return;

        var isWritten = false;
        try
        {
            isWritten = await pieceBuffer.WritePieceAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _requestBlocks.TryRemove(receiveBlock.Index, out _);
            if (_pieceBuffers.TryRemove(receiveBlock.Index, out var removedBuffer))
            {
                removedBuffer.Dispose();
            }
        }

        if (isWritten)
        {
            myBitfield.SetPiece(receiveBlock.Index);
        }
        else
        {
            // Retry with a fresh buffer
            _pieceBuffers[receiveBlock.Index] = new PieceBuffer(receiveBlock.Index, fileManager);
            transfer.AddDiscardedBytes(pieceBuffer.Size);
        }
    }

    public RequestBlock? GetBlock(Bitfield bitfield)
    {
        HashSet<int> excludedIndices = [];
        lock (_requestBlocksLock)
        {
            foreach (
                var requestBlock in _requestBlocks.Values.AsValueEnumerable().SelectMany(i => i)
            )
            {
                excludedIndices.Add(requestBlock.Index);

                if (
                    requestBlock.State == RequestBlockState.Pending
                    && bitfield.HasPiece(requestBlock.Index)
                )
                    return requestBlock;
            }
        }

        var piece = GetPiece(bitfield, excludedIndices);

        if (piece is null)
            return null;

        var blockCount = fileManager.GetBlockCountByPieceIndex(piece.Value);

        lock (_requestBlocksLock)
        {
            if (!_requestBlocks.TryGetValue(piece.Value, out var requestBlocks))
            {
                requestBlocks = new RequestBlock[blockCount];
                _requestBlocks[piece.Value] = requestBlocks;

                for (int i = 0; i < blockCount; i++)
                {
                    requestBlocks[i] = fileManager.GetRequestBlockByBlockIndex(piece.Value, i);
                }
            }

            return requestBlocks[0];
        }
    }

    public RequestBlock[] GetTimeoutRequestBlocks()
    {
        var timeout = TimeoutSeconds.Seconds;
        var now = DateTime.UtcNow;
        lock (_requestBlocksLock)
        {
            return
            [
                .. _requestBlocks
                    .Values.AsValueEnumerable()
                    .SelectMany(i => i)
                    .Where(i =>
                        i?.State == RequestBlockState.Requested
                        && i.RequestedAt is not null
                        && (now - i.RequestedAt.Value) > timeout
                    )
                    .Cast<RequestBlock>(),
            ];
        }
    }

    public PeerConnection GetLastRequester(RequestBlock requestBlock)
    {
        lock (_requestBlocksLock)
        {
            return requestBlock.RequestedFrom[^1];
        }
    }

    public void SetBlockToPending(RequestBlock requestBlock)
    {
        lock (_requestBlocksLock)
        {
            requestBlock.State = RequestBlockState.Pending;
            requestBlock.RequestedAt = null;
        }
    }

    public void SetBlockToRequested(RequestBlock requestBlock, PeerConnection peerConnection)
    {
        lock (_requestBlocksLock)
        {
            requestBlock.State = RequestBlockState.Requested;
            requestBlock.RequestedFrom.Add(peerConnection);
            requestBlock.RequestedAt = DateTimeOffset.UtcNow;
        }
    }

    private int? GetPiece(Bitfield peerBitfield, HashSet<int> excluded)
    {
        var posiblePieces = new List<int>(myBitfield.Length);
        for (int i = 0; i < peerBitfield.Length; i++)
        {
            if (peerBitfield.HasPiece(i) && !myBitfield.HasPiece(i) && !excluded.Contains(i))
            {
                posiblePieces.Add(i);
            }
        }

        if (posiblePieces.Count == 0)
            return null;

        (int index, int rarity)[] posiblePiecesWithRarity = posiblePieces
            .AsValueEnumerable()
            .Select(index => (index, _pieceRarity[index]))
            .OrderBy(i => i.Item2)
            .ToArray();

        //Get 10% or the first 500 of rarest -> shuffle -> take first
        var rarestCount = Math.Min(posiblePiecesWithRarity.Length / 10, 500);
        rarestCount = rarestCount < 1 ? 1 : rarestCount;

        var selectedPiece = posiblePiecesWithRarity
            .AsValueEnumerable()
            .Take(rarestCount)
            .Shuffle()
            .First();

        return selectedPiece.index;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var item in _pieceBuffers)
        {
            item.Value.Dispose();
        }
    }
}
