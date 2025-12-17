using Netorrent.Other;
using Netorrent.P2P.Messages;

namespace Netorrent.IO;

internal interface IPieceWriter : IDisposable
{
    int BlockSize { get; }
    long TotalSize { get; }

    int GetBlockCountByPieceIndex(int pieceIndex);
    int GetPieceSize(int pieceIndex);
    RequestBlock GetRequestBlockByBlockIndex(int pieceIndex, int blockIndex);
    ValueTask<RentedArray<byte>> ReadAsync(
        int pieceIndex,
        int begin,
        int length,
        CancellationToken ct
    );
    ValueTask<bool> VerifyPieceAsync(
        int pieceIndex,
        ReadOnlyMemory<byte> pieceData,
        CancellationToken ct
    );
    ValueTask WriteAsync(
        int pieceIndex,
        int begin,
        ReadOnlyMemory<byte> pieceData,
        CancellationToken ct
    );
}
