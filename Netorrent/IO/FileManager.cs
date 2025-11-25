using System.Buffers;
using System.Security.Cryptography;
using Microsoft.Win32.SafeHandles;
using Netorrent.Other;
using Netorrent.P2P.Messages;
using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.IO;

internal class FileManager : IDisposable
{
    private readonly string _outputDirectory;
    private readonly List<TorrentFileEntry> _files = [];
    private readonly int _pieceLength;
    private readonly List<byte[]> _pieceHashes;
    public Bitfield BitField { get; private set; }

    public const int BlockSize = 16 * 1024;

    public long TotalSize { get; }
    public int MaxBlocksByPiece { get; }
    public string OutputDirectory => _outputDirectory;

    public FileManager(
        string outputDirectory,
        List<InfoFile> torrentFiles,
        int pieceLength,
        List<byte[]> pieceHashes,
        Bitfield bitField
    )
    {
        _outputDirectory = outputDirectory;
        _pieceLength = pieceLength;
        _pieceHashes = pieceHashes;
        BitField = bitField;

        long offset = 0;
        foreach (var item in torrentFiles)
        {
            string fullPath = Path.Combine([_outputDirectory, .. item.Path]);
            var folder = Path.GetDirectoryName(fullPath);

            if (folder is not null)
                Directory.CreateDirectory(folder);

            var handle = File.OpenHandle(
                fullPath,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.ReadWrite,
                options: FileOptions.Asynchronous | FileOptions.RandomAccess
            );
            _files.Add(new TorrentFileEntry(fullPath, offset, item.Length, handle));
            offset += item.Length;
        }

        TotalSize = _files.Sum(f => f.Length);
        MaxBlocksByPiece = _pieceLength / BlockSize;
    }

    public int GetBlockCountByPieceIndex(int pieceIndex)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(pieceIndex);

        var pieceCount = (TotalSize + _pieceLength - 1) / _pieceLength;

        if (pieceIndex >= pieceCount)
            throw new ArgumentOutOfRangeException(nameof(pieceIndex));

        var pieceLength =
            (pieceIndex == pieceCount - 1)
                ? TotalSize - (long)pieceIndex * _pieceLength
                : _pieceLength;

        int blockCount = (int)((pieceLength + BlockSize - 1) / BlockSize);
        return blockCount;
    }

    public RequestBlock GetRequestBlockByBlockIndex(int pieceIndex, int blockIndex)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(pieceIndex);
        ArgumentOutOfRangeException.ThrowIfNegative(blockIndex);
        var pieceCount = (TotalSize + _pieceLength - 1) / _pieceLength;
        if (pieceIndex >= pieceCount)
            throw new ArgumentOutOfRangeException(nameof(pieceIndex));
        var pieceLength =
            (pieceIndex == pieceCount - 1)
                ? TotalSize - (long)pieceIndex * _pieceLength
                : _pieceLength;
        int blockCount = (int)((pieceLength + BlockSize - 1) / BlockSize);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(blockIndex, blockCount);
        int begin = blockIndex * BlockSize;
        int length = (int)Math.Min(BlockSize, pieceLength - begin);
        return new RequestBlock(pieceIndex, begin, length);
    }

    public ulong GetWrittenBytes()
    {
        long total = 0;
        for (int i = 0; i < BitField.Length; i++)
        {
            if (BitField[i])
            {
                long pieceSize = Math.Min(
                    _pieceLength,
                    _files.Sum(f => f.Length) - (long)i * _pieceLength
                );
                total += pieceSize;
            }
        }
        return (ulong)total;
    }

    public ulong GetMissingBytes()
    {
        var totalSize = (ulong)_files.Sum(f => f.Length);
        return totalSize - GetWrittenBytes();
    }

    public async ValueTask WritePieceAsync(
        int pieceIndex,
        int begin,
        ReadOnlyMemory<byte> pieceData,
        CancellationToken ct = default
    )
    {
        long globalOffset = (long)pieceIndex * _pieceLength;
        globalOffset += begin;
        await WriteAsync(globalOffset, pieceData, ct);
    }

    public async ValueTask<bool> VerifyPieceAsync(
        int pieceIndex,
        Memory<byte> pieceData,
        CancellationToken ct = default
    )
    {
        var expectedHash = _pieceHashes[pieceIndex];
        long offset = (long)pieceIndex * _pieceLength;
        int length = _pieceLength;

        var actualHash =
            pieceData.Length > 1024 * 1024
                ? await Task.Run(() => SHA1.HashData(pieceData.Span), ct)
                : SHA1.HashData(pieceData.Span);

        return expectedHash.SequenceEqual(actualHash);
    }

    public async ValueTask ClearPieceAsync(int pieceIndex, CancellationToken ct)
    {
        // Calculate where the piece starts in the torrent
        long globalOffset = (long)pieceIndex * _pieceLength;
        long remaining = _pieceLength;

        // Reuse a shared zero buffer instead of allocating new arrays per write
        byte[] zeroBuffer = [];

        foreach (var file in _files)
        {
            if (globalOffset >= file.EndOffset)
                continue;

            long fileOffset = Math.Max(0, globalOffset - file.StartOffset);
            long writable = Math.Min(remaining, file.Length - fileOffset);

            // lazily allocate a zero buffer large enough for current write
            if (zeroBuffer.Length < writable)
                zeroBuffer = new byte[writable];

            Directory.CreateDirectory(Path.GetDirectoryName(file.FullPath)!);

            await RandomAccess.WriteAsync(
                file.SafeHandle,
                zeroBuffer.AsMemory(0, (int)writable),
                fileOffset,
                ct
            );

            globalOffset += writable;
            remaining -= writable;

            if (remaining <= 0)
                break;
        }
    }

    private async ValueTask WriteAsync(
        long globalOffset,
        ReadOnlyMemory<byte> data,
        CancellationToken ct
    )
    {
        long remaining = data.Length;
        int position = 0;

        foreach (var file in _files)
        {
            if (globalOffset >= file.EndOffset)
                continue;

            long fileOffset = Math.Max(0, globalOffset - file.StartOffset);
            long writable = Math.Min(remaining, file.Length - fileOffset);

            if (!file.isDirectoryCreated)
            {
                Directory.CreateDirectory(Path.GetDirectoryName(file.FullPath)!);
                file.isDirectoryCreated = true;
            }

            await RandomAccess.WriteAsync(
                file.SafeHandle,
                data.Slice(position, (int)writable),
                fileOffset,
                ct
            );

            globalOffset += writable;
            position += (int)writable;
            remaining -= writable;

            if (remaining <= 0)
                break;
        }
    }

    public async ValueTask<RentedArray<byte>> ReadPieceAsync(
        int pieceIndex,
        int begin,
        int length,
        CancellationToken ct = default
    )
    {
        long offset = (long)pieceIndex * _pieceLength;
        offset += begin;
        return await ReadAsync(offset, length, ct);
    }

    private async ValueTask<RentedArray<byte>> ReadAsync(
        long globalOffset,
        int length,
        CancellationToken ct
    )
    {
        var array = ArrayPool<byte>.Shared.Rent(length);
        var buffer = array.AsMemory()[..length];
        int totalRead = 0;

        foreach (var file in _files)
        {
            if (globalOffset >= file.EndOffset)
                continue;

            long fileOffset = Math.Max(0, globalOffset - file.StartOffset);
            long readable = Math.Min(length - totalRead, file.Length - fileOffset);

            int bytesRead = await RandomAccess.ReadAsync(
                file.SafeHandle,
                buffer.Slice(totalRead, (int)readable),
                fileOffset,
                ct
            );

            totalRead += bytesRead;
            globalOffset += bytesRead;

            if (totalRead >= length || bytesRead == 0)
                break;
        }

        if (totalRead < length)
            length = totalRead;

        return new RentedArray<byte>(array, length);
    }

    public void Dispose()
    {
        foreach (var item in _files)
        {
            try
            {
                RandomAccess.FlushToDisk(item.SafeHandle);
            }
            finally
            {
                item.SafeHandle.Dispose();
            }
        }
    }

    private sealed record TorrentFileEntry(
        string FullPath,
        long StartOffset,
        long Length,
        SafeFileHandle SafeHandle
    )
    {
        public long EndOffset => StartOffset + Length;
        public bool isDirectoryCreated { get; set; } = false;
    }
}
