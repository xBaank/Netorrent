using Netorrent.P2P.Messages;
using ZLinq;

namespace Netorrent.P2P.Download;

internal class PiecePicker(Bitfield myBitfield)
{
    private readonly int[] _pieceRarity = new int[myBitfield.Length];

    public void IncreaseRarity(int index)
    {
        Interlocked.Increment(ref _pieceRarity[index]);
    }

    public void DecreaseRarity(int index)
    {
        Interlocked.Decrement(ref _pieceRarity[index]);
    }

    public int? GetRarestPiece(Bitfield peerBitfield, ICollection<int> excluded)
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
