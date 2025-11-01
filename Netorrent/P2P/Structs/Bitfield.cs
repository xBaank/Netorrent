using System.Buffers;
using System.Collections;

namespace Netorrent.P2P.Structs;

internal class Bitfield
{
    private readonly BitArray _bits;

    public Bitfield(int pieceCount, bool isInitialized = false)
    {
        _bits = new BitArray(pieceCount, isInitialized);
    }

    public Bitfield(byte[] bytes)
    {
        _bits = new BitArray(bytes);
    }

    public int Length => _bits.Length;

    public bool this[int index]
    {
        get => _bits[index];
        set => _bits[index] = value;
    }

    public void HavePiece(int index)
    {
        if (index < _bits.Length)
            _bits[index] = true;
    }

    public MemoryRented<byte> ToMemoryRented()
    {
        if (!_bits.HasAnySet())
            return MemoryRented<byte>.From([]);

        int byteCount = (_bits.Length + 7) / 8;
        var owner = MemoryPool<byte>.Shared.Rent(byteCount);
        var memory = owner.Memory[..byteCount];
        PackBitsBigEndian(memory.Span);

        return new MemoryRented<byte>(owner, byteCount);
    }

    void PackBitsBigEndian(Span<byte> dest)
    {
        int byteLen = (_bits.Length + 7) / 8;
        if (dest.Length < byteLen)
            throw new ArgumentException("dest too small", nameof(dest));
        dest[..byteLen].Clear();

        for (int i = 0; i < _bits.Length; i++)
        {
            if (_bits[i])
                dest[i / 8] |= (byte)(1 << (7 - (i % 8)));
        }
    }
}
