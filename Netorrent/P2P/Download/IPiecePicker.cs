using System.Diagnostics.CodeAnalysis;
using Netorrent.P2P.Messages;

namespace Netorrent.P2P.Download;

internal interface IPiecePicker : IAsyncDisposable
{
    int BlockSize { get; }

    void CompletePiece(int index);
    void CompleteRequestBlock(RequestBlock requestBlock);
    void DecreaseRarity(int index);
    RequestBlock? GetBlock(Bitfield bitfield);
    int GetBlockCountByPieceIndex(int pieceIndex);
    PeerConnection GetLastRequester(RequestBlock requestBlock);
    int GetPieceSize(int pieceIndex);
    RequestBlock GetRequestBlockByBlockIndex(int pieceIndex, int blockIndex);
    RequestBlock[] GetTimeoutRequestBlocks();
    void IncreaseRarity(int index);
    void SetBlockToPending(RequestBlock requestBlock);
    void SetBlockToRequested(RequestBlock requestBlock, PeerConnection peerConnection);
    bool TryGetRequestedBlock(
        Block receiveBlock,
        [NotNullWhen(true)] out RequestBlock? requestBlock
    );
}
