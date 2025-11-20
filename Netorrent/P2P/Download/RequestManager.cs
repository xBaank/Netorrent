using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.IO.Pipelines;
using System.Net;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using Netorrent.Extensions;
using Netorrent.IO;
using Netorrent.P2P.Messages;
using ZLinq;

namespace Netorrent.P2P.Download;

internal class RequestManager(
    IReadOnlyDictionary<IPEndPoint, PeerConnection> activePeers,
    Bitfield myBitfield,
    FileManager fileManager
) : IAsyncDisposable
{
    const int MinPeersForRarity = 6;
    const int WarmupTimeoutMs = 8000;

    private readonly Channel<Block> _receiveBlocks = Channel.CreateBounded<Block>(
        new BoundedChannelOptions(fileManager.MaxBlocksByPiece * 5)
        {
            SingleWriter = true,
            SingleReader = true,
        }
    );
    private readonly Lock _rarityLock = new();
    private readonly uint[] _pieceRarity = new uint[myBitfield.Length];
    private readonly Dictionary<uint, HashSet<int>> rarityBuckets = [];
    private readonly ConcurrentDictionary<int, PieceState> _pieceStates = [];
    private readonly ConcurrentDictionary<int, PieceBuffer> _pieceBuffers = [];
    private readonly Channel<PeerConnection> _scheduleChannel =
        Channel.CreateUnbounded<PeerConnection>();
    private Task? _requestManagerTask;

    public Task? WaitTask => _requestManagerTask;

    public void Start(CancellationToken cancellationToken) =>
        _requestManagerTask ??= RunRequestManagerAsync(cancellationToken);

    public async Task RunRequestManagerAsync(CancellationToken cancellationToken)
    {
        var faultedTask = await Task.WhenAny(
            ReceiveBlocksAsync(cancellationToken),
            ReScheduleTimedoutBlocksAsync(cancellationToken),
            SchedulePiecesAsync(cancellationToken)
        );
        await faultedTask;
    }

    public async Task ReceiveBlocksAsync(CancellationToken cancellationToken)
    {
        await foreach (var receiveBlock in _receiveBlocks.Reader.ReadAllAsync(cancellationToken))
        {
            if (!_pieceBuffers.TryGetValue(receiveBlock.Index, out var pieceBuffer))
            {
                pieceBuffer = new PieceBuffer(receiveBlock.Index, fileManager);
                _pieceBuffers[receiveBlock.Index] = pieceBuffer;
            }

            pieceBuffer.AddBlock(receiveBlock);

            if (!pieceBuffer.IsComplete)
                continue;

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
                }
                else
                {
                    await myBitfield.SetPieceAsync(receiveBlock.Index, cancellationToken);
                }
            }
            finally
            {
                pieceBuffer.Dispose();
            }
        }
    }

    public async Task ReScheduleTimedoutBlocksAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(1.Seconds, cancellationToken);
        }
    }

    public async Task SchedulePiecesAsync(CancellationToken cancellationToken)
    {
        var warmupTask = Task.Delay(WarmupTimeoutMs, cancellationToken);
        var minPeersReady = 0;
        do
        {
            await Task.Delay(100.Milliseconds, cancellationToken);
            minPeersReady = activePeers.Values.Count(i => i.AmInterested);
        } while (!warmupTask.IsCompletedSuccessfully && minPeersReady < MinPeersForRarity);

        await foreach (
            var peerConnection in _scheduleChannel.Reader.ReadAllAsync(cancellationToken)
        )
        {
            var activePieces = rarityBuckets
                .AsValueEnumerable()
                .OrderBy(kvp => kvp.Key)
                .SelectMany(kvp => kvp.Value)
                .Take(250)
                .ToArray();

            if (!peerConnection.AmInterested)
                continue;

            await ScheduleRequests(activePieces, peerConnection, cancellationToken);
        }
    }

    private async Task ScheduleRequests(
        int[] pieces,
        PeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        var max = peerConnection.DownloadSpeedTracker.CurrentBps.Kbps switch
        {
            >= 1000 => 32,
            >= 500 => 16,
            >= 200 => 12,
            >= 50 => 10,
            _ => 8,
        };

        while (peerConnection.RequestedBlocksCount < max)
        {
            var block = SelectBlock(peerConnection, pieces);
            if (block is null)
                break;

            block.State = RequestBlockState.Requested;
            block.RequestedFrom = peerConnection;
            block.RequestedAt = DateTimeOffset.UtcNow;
            await peerConnection.AddRequestAsync(block, cancellationToken);
        }
    }

    private RequestBlock? SelectBlock(PeerConnection peerConnection, int[] pieces)
    {
        foreach (var index in pieces)
        {
            if (!_pieceStates.TryGetValue(index, out var piece))
            {
                piece = new((byte)fileManager.GetBlockCountByPieceIndex(index));
                _pieceStates[index] = piece;
            }

            if (piece.IsComplete)
                continue;

            if (!peerConnection.PeerBitField.HasPiece(index))
                continue;

            for (int i = 0; i < piece.BlockCount; i++)
            {
                bool isRequested = piece.HasRequestedBlock(i);
                bool isReceived = piece.HasReceivedBlock(i);

                if (!isRequested && !isReceived)
                {
                    // mark block as requested
                    piece.RequestBlock(i);

                    var requestBlock = fileManager.GetRequestBlockByBlockIndex(index, i);
                    requestBlock.RequestedAt = DateTimeOffset.UtcNow;
                    requestBlock.RequestedFrom = peerConnection;
                    return requestBlock;
                }
            }
        }

        return null;
    }

    internal void IncreaseRarity(int index)
    {
        lock (_rarityLock)
        {
            var old = _pieceRarity[index];
            var now = ++_pieceRarity[index];

            rarityBuckets[old].Remove(index);
            rarityBuckets[now].Add(index);
        }
    }

    internal void DecreaseRarity(int index)
    {
        lock (_rarityLock)
        {
            var old = _pieceRarity[index];
            var now = --_pieceRarity[index];

            rarityBuckets[old].Remove(index);
            rarityBuckets[now].Add(index);
        }
    }

    internal void OnPeerInterested(PeerConnection peer) => _scheduleChannel.Writer.TryWrite(peer);

    internal async ValueTask ReceiveBlockAsync(
        Block block,
        PeerConnection receiver,
        CancellationToken cancellationToken
    )
    {
        _scheduleChannel.Writer.TryWrite(receiver);
        await _receiveBlocks.Writer.WriteAsync(block, cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        _receiveBlocks.Writer.TryComplete();
        await foreach (var item in _receiveBlocks.Reader.ReadAllAsync())
        {
            item.Dispose();
        }
        foreach (var item in _pieceBuffers)
        {
            item.Value.Dispose();
        }
    }
}
