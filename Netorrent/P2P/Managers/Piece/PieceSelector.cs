using System.Net;
using Netorrent.P2P.Messages;

namespace Netorrent.P2P.Managers.Piece;

internal class PieceSelector(
    IReadOnlyDictionary<IPEndPoint, PeerConnection> peers,
    Bitfield myBitfield
)
{
    private readonly IReadOnlyDictionary<IPEndPoint, PeerConnection> _peers = peers;
    private readonly Bitfield _myBitfield = myBitfield;
    private readonly Random _rng = new();
    private IEnumerable<Bitfield> Bitfields => _peers.Values.Select(i => i.PeerBitField);

    public int? GetNextRarestPiece(int rarestSampleSize = 5)
    {
        var availability = GetPieceAvailability();

        var excluded = _peers
            .Values.Where(i => i.MyBitField != _myBitfield)
            .Select(pc => pc.CurrentPieceDownloading)
            .Where(piece => piece.HasValue)
            .Select(piece => piece!.Value)
            .ToHashSet();

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

    private Dictionary<int, int> GetPieceAvailability()
    {
        int pieceCount = _myBitfield.Length;
        var availability = new int[pieceCount];

        foreach (var peerBits in Bitfields)
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
}
