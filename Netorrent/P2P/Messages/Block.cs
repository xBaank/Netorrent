using Netorrent.IO;
using Netorrent.Other;

namespace Netorrent.P2P.Messages;

internal class Block(int index, int begin, RentedArray<byte> payload, PeerConnection fromPeer)
    : IDisposable
{
    public readonly int Index = index;
    public readonly int Begin = begin;
    public readonly RentedArray<byte> Payload = payload;
    public readonly PeerConnection FromPeer = fromPeer;
    public readonly DateTimeOffset ReceivedAt = DateTimeOffset.UtcNow;

    public int BlockIndex => Begin / FileManager.BlockSize;

    public void Dispose()
    {
        Payload.Dispose();
    }
}
