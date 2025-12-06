using System.Collections.Concurrent;
using System.Threading;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Messages;
using ZLinq;

namespace Netorrent.P2P.Download;

internal class PiecePicker(Bitfield myBitfield, FileManager fileManager)
{
    const int TimeoutSeconds = 10;

    private readonly int[] _pieceRarity = new int[myBitfield.Length];
    private readonly ConcurrentDictionary<int, RequestBlock?[]> _requestBlocksByPieceIndex = [];
    private readonly ConcurrentDictionary<int, PieceBuffer> _pieceBuffers = [];

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

        pieceBuffer.AddBlock(receiveBlock);

        if (_requestBlocksByPieceIndex.TryGetValue(receiveBlock.Index, out var requestBlocks))
        {
            var requestBlock = requestBlocks.FirstOrDefault(i => i?.Begin == receiveBlock.Begin);
            if (
                requestBlock is not null
                && requestBlock.RequestedFrom.Contains(receiveBlock.FromPeer)
            )
            {
                var rtt = requestBlock.RequestedAt.HasValue
                    ? receiveBlock.ReceivedAt - requestBlock.RequestedAt.Value
                    : TimeoutSeconds.Seconds;
                receiveBlock.FromPeer.PeerRequestWindow.CalculateWindow(
                    (long)receiveBlock.FromPeer.DownloadSpeedTracker.CurrentBps.Bps,
                    rtt
                );
            }

            receiveBlock.FromPeer.RequestedBlocksCount--;
            requestBlock?.State = RequestBlockState.Completed;
            requestBlock?.RequestedAt = null;
            requestBlock?.RequestedFrom.Clear();
        }

        if (!pieceBuffer.IsComplete)
            return;

        try
        {
            var isWritten = await pieceBuffer.WritePieceAsync(cancellationToken);
            _pieceBuffers.TryRemove(receiveBlock.Index, out _);

            if (!isWritten)
            {
                _pieceBuffers[receiveBlock.Index] = new PieceBuffer(
                    receiveBlock.Index,
                    fileManager
                );
                _requestBlocksByPieceIndex.TryRemove(receiveBlock.Index, out _);
            }
            else
            {
                myBitfield.SetPiece(receiveBlock.Index, cancellationToken);
                _requestBlocksByPieceIndex.TryRemove(receiveBlock.Index, out _);
            }
        }
        finally
        {
            pieceBuffer.Dispose();
            _pieceBuffers.TryRemove(receiveBlock.Index, out _);
        }
    }

    public RequestBlock[] GetTimeoutRequestBlocks() =>
        _requestBlocksByPieceIndex
            .Values.AsValueEnumerable()
            .SelectMany(i => i)
            .Where(i =>
                i is not null
                && i.State == RequestBlockState.Pending
                && (DateTimeOffset.UtcNow - i.RequestedAt) > TimeoutSeconds.Seconds
            )
            .Cast<RequestBlock>()
            .ToArray();

    public int? GetRarestPiece(Bitfield peerBitfield)
    {
        var posiblePieces = new List<int>(myBitfield.Length);
        for (int i = 0; i < peerBitfield.Length; i++)
        {
            if (
                peerBitfield.HasPiece(i)
                && !myBitfield.HasPiece(i)
                && !_requestBlocksByPieceIndex.ContainsKey(i)
            )
            {
                posiblePieces.Add(i);
            }
        }

        if (posiblePieces.Count == 0)
            return null;

        (int index, int rarity)[] posiblePiecesWithRarity = posiblePieces
            .AsValueEnumerable()
            .Select(index => (index, _pieceRarity[index]))
            .OrderByDescending(i => i.Item2)
            .ToArray();

        //Get 10% or the first 500 of rarest -> shuffle -> take first
        var rarestCount = Math.Min(posiblePiecesWithRarity.Length / 10, 500);

        var selectedPiece = posiblePiecesWithRarity
            .AsValueEnumerable()
            .Take(rarestCount)
            .Shuffle()
            .First();

        return selectedPiece.index;
    }
}
