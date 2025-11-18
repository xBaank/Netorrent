using System.Net;
using Netorrent.IO;
using Netorrent.P2P.Messages;
using ZLinq;

namespace Netorrent.P2P.Managers;

internal class RequestManager(
    IReadOnlyDictionary<IPEndPoint, PeerConnection> activePeers,
    Bitfield myBitfield,
    FileManager fileManager
)
{
    public void Start() { }

    public async Task ScheduleBlocksTask(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var peersWithMissingPieces = activePeers
                .Values.AsValueEnumerable()
                .Where(i => myBitfield.HasAnyMissingPiece(i.PeerBitField))
                .ToArray();

            var allBitfields = peersWithMissingPieces
                .AsValueEnumerable()
                .Select(i => i.PeerBitField);

            var rarityByPieces = new Dictionary<int, int>();

            for (var i = 0; i < myBitfield.Length; i++)
            {
                rarityByPieces[i] = 0;
            }

            foreach (var pieces in allBitfields.Select(i => i.Pieces))
            {
                for (int i = 0; i < pieces.Count; i++)
                {
                    if (pieces[i])
                        rarityByPieces[i]++;
                }
            }
            var mostRarePieces = rarityByPieces
                .AsValueEnumerable()
                .OrderBy(i => i.Value)
                .Take(10)
                .OrderBy(_ => Random.Shared.Next())
                .Take(5)
                .ToArray();

            foreach (var (rarePiece, _) in mostRarePieces)
            {
                var avaliablePeers = peersWithMissingPieces
                    .AsValueEnumerable()
                    .Where(i => i.PeerBitField.HasPiece(rarePiece))
                    .ToList();

                var requestBlocks = fileManager.GetBlocksByPieceIndex(rarePiece);
            }
        }
    }
}
