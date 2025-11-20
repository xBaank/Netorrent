namespace Netorrent.P2P.Download;

internal class PieceState
{
    public PieceState(byte blockCount)
    {
        BlockCount = blockCount;
    }

    public ulong RequestedMask;
    public ulong ReceivedMask;
    public byte BlockCount;

    public void ReceiveBlock(int index) => ReceivedMask |= 1u << index;

    public void RequestBlock(int index) => RequestedMask |= 1u << index;

    public void ClearBlock(int index) => RequestedMask &= ~(1u << index);

    public bool HasReceivedBlock(int index) => (ReceivedMask & (1u << index)) != 0;

    public bool HasRequestedBlock(int index) => (RequestedMask & (1u << index)) != 0;

    public bool IsComplete => ReceivedMask == (1u << BlockCount) - 1;
}
