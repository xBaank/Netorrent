using Netorrent.Other;

namespace Netorrent.P2P.Managers.Piece;

internal readonly struct Block(int index, int begin, RentedArray<byte> payload) : IDisposable
{
    public readonly int Index = index;
    public readonly int Begin = begin;
    public readonly RentedArray<byte> Payload = payload;

    public void Dispose()
    {
        Payload.Dispose();
    }
}
