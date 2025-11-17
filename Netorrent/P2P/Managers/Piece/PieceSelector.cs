using System.Net;
using Netorrent.P2P.Messages;
using ZLinq;
using ZLinq.Linq;

namespace Netorrent.P2P.Managers.Piece;

internal class PieceSelector(
    IReadOnlyDictionary<IPEndPoint, PeerConnection> peers,
    Bitfield myBitfield
)
{
    private readonly IReadOnlyDictionary<IPEndPoint, PeerConnection> _peers = peers;
    private readonly Bitfield _myBitfield = myBitfield;
    private readonly Random _rng = new();
    private readonly Lock _lock = new();
    private ValueEnumerable<
        Select<FromEnumerable<PeerConnection>, PeerConnection, Bitfield>,
        Bitfield
    > Bitfields => _peers.Values.AsValueEnumerable().Select(i => i.PeerBitField);

    public async Task OnPeerDisconnected(CancellationToken cancellationToken)
    {
        int? index;
        lock (_lock)
        {
            index = GetNextRarestPiece();
            if (index is null)
                return;
        }

        var peerWithoutPiece = _peers
            .Values.AsValueEnumerable()
            .Where(i => i.CurrentPieceIndex is null)
            .Where(i => i.PeerBitField.HasPiece(index.Value))
            .FirstOrDefault();

        if (peerWithoutPiece is null)
            return;

        await peerWithoutPiece.SetPieceToDownloadAsync(index.Value, cancellationToken);
    }

    public async Task OnPeerRequestPiece(
        PeerConnection peerConnection,
        CancellationToken cancellationToken
    )
    {
        int? index;
        lock (_lock)
        {
            index = GetNextRarestPiece();
            if (index is null)
                return;
        }

        await peerConnection.SetPieceToDownloadAsync(index.Value, cancellationToken);
    }

    public int? GetNextRarestPiece(
        IReadOnlySet<int>? limitedPieceSet = null,
        int rarestSampleSize = 5
    )
    {
        var availability = GetPieceAvailability();

        var excluded = _peers
            .Values.AsValueEnumerable()
            .Where(i => i.MyBitField != _myBitfield)
            .Select(pc => pc.CurrentPieceIndex)
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

        var sorted = availability.AsValueEnumerable().OrderBy(kv => kv.Value).ToList();
        var subset = sorted
            .AsValueEnumerable()
            .Where(i => limitedPieceSet?.Contains(i.Key) ?? true)
            .Take(rarestSampleSize)
            .ToList();
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
