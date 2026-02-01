using System.Diagnostics.CodeAnalysis;
using Netorrent.P2P.Messages;

namespace Netorrent.P2P.Download;

internal interface IPiecePicker
{
    int BlockSize { get; }
    bool IsEndGame { get; }

    void IncreaseRarity(int index);
    void DecreaseRarity(int index);
    void CompletePiece(int index);
    bool TryGetRequestBlock(
        IPeerConnection peerConnection,
        [NotNullWhen(true)] out RequestBlock? requestBlock
    );

    int GetBlockCountByPieceIndex(int pieceIndex);
    int GetPieceSize(int pieceIndex);
    long GetBitfieldSize();
    RequestBlock GetRequestBlockByBlockIndex(int pieceIndex, int blockIndex);
    IEnumerable<RequestBlock> GetTimeoutRequestBlocks();
    bool TryGetRequestedBlock(
        Block receiveBlock,
        [NotNullWhen(true)] out RequestBlock? requestBlock
    );
}
