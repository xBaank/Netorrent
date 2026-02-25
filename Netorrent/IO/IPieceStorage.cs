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
    bool VerifyPiece(int pieceIndex, ReadOnlyMemory<byte> pieceData);
    bool VerifyPieceHash(int pieceIndex, ref readonly ReadOnlySpan<byte> computedHash);

    ValueTask WriteAsync(
        int pieceIndex,
        int begin,
        ReadOnlyMemory<byte> pieceData,
        CancellationToken ct
    );
}
