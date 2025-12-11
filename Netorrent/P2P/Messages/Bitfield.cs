using System.Buffers;
using System.Collections;
using System.Reactive.Subjects;
using Netorrent.Other;

namespace Netorrent.P2P.Messages;

internal class Bitfield
{
    private readonly Lock _lock = new();
    private readonly BitArray _bits;
    private readonly Subject<int> _stateChanged = new();
    public IObservable<int> StateChanged => _stateChanged;

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

    public int Length
    {
        get
        {
            lock (_lock)
            {
                return _bits.Length;
            }
        }
    }

    public bool IsComplete
    {
        get
        {
            lock (_lock)
            {
                return _bits.HasAllSet();
            }
        }
    }

    public bool HasPiece(int index)
    {
        lock (_lock)
        {
            return index < _bits.Length && _bits[index];
        }
    }

    internal void SetPiece(int index)
    {
        lock (_lock)
        {
            if (index >= _bits.Length)
                return;

            if (_bits[index])
                return;

            _bits[index] = true;
            _stateChanged.OnNext(index);

            if (IsComplete)
                _stateChanged.OnCompleted();
        }
    }

    internal bool HasAnyMissingPiece(Bitfield other)
    {
        if (other.Length != Length)
            throw new ArgumentException("Bitfields must have the same length.", nameof(other));

        for (int i = 0; i < Length; i++)
        {
            // If the peer has the piece and I don't, I'm interested
            if (other.HasPiece(i) && !HasPiece((i)))
                return true;
        }

        return false;
    }

    internal RentedArray<byte> ToRentedArray()
    {
        lock (_lock)
        {
            int byteCount = (_bits.Length + 7) / 8;
            var array = ArrayPool<byte>.Shared.Rent(byteCount);
            var memory = array.AsSpan()[..byteCount];
            PackBitsBigEndian(memory);

            return new RentedArray<byte>(array, byteCount);
        }
    }

    private void PackBitsBigEndian(Span<byte> dest)
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
