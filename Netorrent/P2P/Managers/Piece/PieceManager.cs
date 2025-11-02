using System.Threading.Channels;
using Netorrent.P2P.Structs;

namespace Netorrent.P2P.Managers.Piece;

internal class PieceManager(Bitfield myBitfield, IReadOnlyList<Bitfield> peersBitfields)
{
    private readonly Bitfield _myBitfield = myBitfield;
    private readonly IReadOnlyList<Bitfield> _peersBitfields = peersBitfields;
    private readonly Random _rng = new();

    private readonly Channel<Block> _pendingRequests = Channel.CreateBounded<Block>(
        new BoundedChannelOptions(128) { SingleWriter = false, SingleReader = true }
    );

    public Dictionary<int, int> GetPieceAvailability()
    {
        int pieceCount = _myBitfield.Length;
        var availability = new int[pieceCount];

        foreach (var peerBits in _peersBitfields)
        {
            for (int i = 0; i < pieceCount; i++)
            {
                if (peerBits[i])
                    availability[i]++;
            }
        }

        var dict = new Dictionary<int, int>(pieceCount);
        for (int i = 0; i < pieceCount; i++)
        {
            if (availability[i] > 0 && !_myBitfield[i])
                dict[i] = availability[i];
        }

        return dict;
    }

    public int? GetNextRarestPiece(HashSet<int>? excluded = null, int rarestSampleSize = 5)
    {
        var availability = GetPieceAvailability();

        if (excluded is not null)
        {
            foreach (var ex in excluded)
                availability.Remove(ex);
        }

        if (availability.Count == 0)
            return null;

        var sorted = availability.OrderBy(kv => kv.Value).ToList();
        var subset = sorted.Take(rarestSampleSize).ToList();
        int choiceIndex = _rng.Next(subset.Count);
        return subset[choiceIndex].Key;
    }
}
