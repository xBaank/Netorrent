namespace Netorrent.P2P.Download;

internal class PieceRarity
{
    public int Rarity
    {
        get => field;
        set => Interlocked.Increment(ref field);
    }
}
