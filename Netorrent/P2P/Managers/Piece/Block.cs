using Netorrent.Other;

namespace Netorrent.P2P.Managers.Piece;

internal readonly struct Block(int index, int begin, MemoryRented<byte> payload) : IDisposable
{
    public readonly int Index = index;
    public readonly int Begin = begin;
    public readonly MemoryRented<byte> Payload = payload;

    public void Dispose()
    {
        Payload.Dispose();
    }
}
