using System.Diagnostics.CodeAnalysis;
using Netorrent.P2P;
using Netorrent.P2P.Download;
using Netorrent.P2P.Messages;

namespace Netorrent.Tests.Fakes;

internal class FakePiecePicker : IPiecePicker
{
    public int BlockSize => 16 * 1024;

    public void CompletePiece(int index)
    {
        throw new NotImplementedException();
    }

    public void CompleteRequestBlock(RequestBlock requestBlock)
    {
        throw new NotImplementedException();
    }

    public void DecreaseRarity(int index) { }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public long GetBitfieldSize()
    {
        throw new NotImplementedException();
    }

    public RequestBlock? GetBlock(Bitfield bitfield)
    {
        throw new NotImplementedException();
    }

    public int GetBlockCountByPieceIndex(int pieceIndex)
    {
        throw new NotImplementedException();
    }

    public int GetPieceSize(int pieceIndex)
    {
        throw new NotImplementedException();
    }

    public RequestBlock GetRequestBlockByBlockIndex(int pieceIndex, int blockIndex)
    {
        throw new NotImplementedException();
    }

    public IEnumerable<RequestBlock> GetTimeoutRequestBlocks()
    {
        throw new NotImplementedException();
    }

    public void IncreaseRarity(int index) { }

    public void SetBlockToPending(RequestBlock requestBlock)
    {
        throw new NotImplementedException();
    }

    public void SetBlockToRequested(RequestBlock requestBlock, IPeerConnection peerConnection)
    {
        throw new NotImplementedException();
    }

    public bool TryGetRequestedBlock(
        Block receiveBlock,
        [NotNullWhen(true)] out RequestBlock? requestBlock
    )
    {
        throw new NotImplementedException();
    }
}
