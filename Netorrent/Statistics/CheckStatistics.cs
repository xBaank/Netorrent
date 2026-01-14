namespace Netorrent.Statistics;

public class CheckStatistics(long totalPiecesCount)
{
    private long _checkedPieces;

    public long CheckedPiecesCount => Interlocked.Read(ref _checkedPieces);
    public long TotalPiecesCount => totalPiecesCount;

    internal void SetCheckedPieces(long pieceCount)
    {
        Interlocked.Exchange(ref _checkedPieces, pieceCount);
    }

    internal void AddCheckedPiece()
    {
        Interlocked.Increment(ref _checkedPieces);
    }

    internal void Reset()
    {
        Interlocked.Exchange(ref _checkedPieces, 0);
    }
}
