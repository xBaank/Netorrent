using System.Buffers;
using Netorrent.IO;
using Netorrent.Other;

internal class FakePieceStorage() : IPieceStorage
{
    public void Dispose() { }

    public bool VerifyPiece(int pieceIndex, ReadOnlyMemory<byte> pieceData) => true;

    public ValueTask WriteAsync(
        int pieceIndex,
        int begin,
        ReadOnlyMemory<byte> pieceData,
        CancellationToken ct
    )
    {
        return ValueTask.CompletedTask;
    }

    public ValueTask<RentedArray<byte>> ReadAsync(
        int pieceIndex,
        int begin,
        int length,
        CancellationToken ct
    )
    {
        var array = ArrayPool<byte>.Shared.Rent(length);
        return ValueTask.FromResult(new RentedArray<byte>(array, length));
    }

    public bool VerifyPieceHash(int pieceIndex, ref readonly ReadOnlySpan<byte> computedHash) =>
        true;
}
