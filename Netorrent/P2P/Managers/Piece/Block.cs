using Netorrent.Other;

namespace Netorrent.P2P.Managers.Piece;

internal readonly struct Block(int index, int begin, MemoryRented<byte> payload)
{
    public readonly int Index = index;
    public readonly int Begin = begin;
    public readonly MemoryRented<byte> payload = payload;
}
