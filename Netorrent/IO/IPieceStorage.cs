using Netorrent.Other;

namespace Netorrent.IO;

internal interface IPieceStorage : IDisposable
{
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
