using System.Buffers;
using System.Collections;
using Netorrent.Other;
using ZLinq;

namespace Netorrent.P2P.Messages;

public class Bitfield
{
    internal event Func<int, CancellationToken, Task>? OnHavePieceAsync;
    private readonly Lock _lock = new();
    private readonly BitArray _bits;

    internal Bitfield(int pieceCount, bool isInitialized = false)
    {
        _bits = new BitArray(pieceCount, isInitialized);
    }

    internal Bitfield(ReadOnlySpan<byte> bytes, int pieceCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(pieceCount);

        _bits = new BitArray(pieceCount);

        for (int i = 0; i < pieceCount; i++)
        {
            int byteIndex = i / 8;
            int bitIndex = 7 - (i % 8); // Most significant byte

            bool bit = (bytes[byteIndex] & (1 << bitIndex)) != 0;
            _bits[i] = bit;
        }
    }

    public int Length => _bits.Length;

    public bool IsComplete => _bits.HasAllSet();

    internal void SetPiece(int index, CancellationToken cancellationToken)
    {
        Func<int, CancellationToken, Task>? callback = null;

        lock (_lock)
        {
            if (index >= _bits.Length)
                return;

            _bits[index] = true;
            callback = OnHavePieceAsync;
        }

        callback?.Invoke(index, cancellationToken);
    }

    internal bool HasPiece(int index) => index < _bits.Length && _bits[index];

    internal bool HasAnyMissingPiece(Bitfield other)
    {
        if (other.Length != Length)
            throw new ArgumentException("Bitfields must have the same length.", nameof(other));

        for (int i = 0; i < Length; i++)
        {
            // If the peer has the piece and I don't, I'm missing something they have
            if (other._bits[i] && !this._bits[i])
                return true;
        }

        return false;
    }

    internal RentedArray<byte> ToRentedArray()
    {
        int byteCount = (_bits.Length + 7) / 8;
        var array = ArrayPool<byte>.Shared.Rent(byteCount);
        var memory = array.AsSpan()[..byteCount];
        PackBitsBigEndian(memory);

        return new RentedArray<byte>(array, byteCount);
    }

    internal void PackBitsBigEndian(Span<byte> dest)
    {
        int byteLen = (_bits.Length + 7) / 8;
        if (dest.Length < byteLen)
            throw new ArgumentException("dest too small", nameof(dest));
        dest[..byteLen].Clear();

        for (int i = 0; i < _bits.Length; i++)
        {
            if (_bits[i])
                dest[i / 8] |= (byte)(1 << 7 - i % 8);
        }
    }
}
