using System.Diagnostics.CodeAnalysis;
using Netorrent.P2P.Messages;
using R3;

namespace Netorrent.P2P.Download;

internal interface IPiecePicker : IAsyncDisposable
{
    int BlockSize { get; }
    bool IsEndGame { get; }

    void CompletePiece(int index);
    void CompleteRequestBlock(RequestBlock requestBlock);
    void DecreaseRarity(int index);
    bool TryGetRequestBlock(Bitfield bitfield, [NotNullWhen(true)] out RequestBlock? requestBlock);

    int GetBlockCountByPieceIndex(int pieceIndex);
    int GetPieceSize(int pieceIndex);
    long GetBitfieldSize();
    RequestBlock GetRequestBlockByBlockIndex(int pieceIndex, int blockIndex);
    IEnumerable<RequestBlock> GetTimeoutRequestBlocks();
    void IncreaseRarity(int index);
    void SetBlockToPending(RequestBlock requestBlock);
    void SetBlockToRequested(RequestBlock requestBlock, IPeerConnection peerConnection);
    bool TryGetRequestedBlock(
        Block receiveBlock,
        [NotNullWhen(true)] out RequestBlock? requestBlock
    );
}
