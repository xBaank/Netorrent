using System.Buffers;
using System.Collections;
using Netorrent.Extensions;
using Netorrent.Other;
using R3;

namespace Netorrent.P2P.Messages;

public class Bitfield
{
    private readonly BitArray _bits;
    private readonly Subject<int> _stateChanged = new();
    internal Subject<int> StateChanged => _stateChanged;
    public int Length => _bits.Length;

    public bool IsComplete => _bits.HasAllSet();

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

    public bool HasPiece(int index) => index < _bits.Length && _bits[index];

    internal void SetPiece(int index)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, _bits.Length, nameof(index));

        if (_bits[index])
        {
            return;
        }

        _bits[index] = true;
        _stateChanged.OnNext(index);

        if (IsComplete)
        {
            _stateChanged.OnCompleted();
        }
    }

    internal void SetPieces(IReadOnlySet<int> indexes)
    {
        foreach (var index in indexes)
        {
            SetPiece(index);
        }
    }

    internal void Reset()
    {
        for (int i = 0; i < _bits.Length; i++)
        {
            _bits[i] = false;
        }
    }

    internal bool HasAnyMissingPiece(Bitfield other)
    {
        ArgumentOutOfRangeException.ThrowIfNotEqual(other.Length, Length, nameof(other));

        for (int i = 0; i < Length; i++)
        {
            // If the peer has the piece and I don't, I'm interested
            if (other.HasPiece(i) && !HasPiece((i)))
            {
                return true;
            }
        }

        return false;
    }

    internal RentedArray<byte> ToRentedArray()
    {
        int byteCount = (_bits.Length + 7) / 8;
        var array = ArrayPool<byte>.Shared.Rent(byteCount);
        try
        {
            var memory = array.AsSpan()[..byteCount];
            PackBitsBigEndian(memory);

            return new RentedArray<byte>(array, byteCount);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(array);
            throw;
        }
    }

    private void PackBitsBigEndian(Span<byte> dest)
    {
        int byteLen = (_bits.Length + 7) / 8;
        ArgumentOutOfRangeException.ThrowIfLessThan(dest.Length, byteLen, nameof(dest));

        dest[..byteLen].Clear();

        for (int i = 0; i < _bits.Length; i++)
        {
            if (_bits[i])
            {
                dest[i / 8] |= (byte)(1 << 7 - i % 8);
            }
        }
    }
}
