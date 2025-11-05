using System.Security.Cryptography;
using Netorrent.P2P.Managers.Request;
using Netorrent.P2P.Messages;
using Netorrent.TorrentFile.FileStructure;

namespace Netorrent.IO;

internal class FileManager : IAsyncDisposable
{
    private readonly string _outputDirectory;
    private readonly List<TorrentFileEntry> _files = [];
    private readonly int _pieceLength;
    private readonly List<byte[]> _pieceHashes;
    public Bitfield BitField { get; private set; }

    public const int BlockSize = 16 * 1024;

    public long TotalSize => _files.Sum(f => f.Length);
    public int MaxBlocksByPiece => _pieceLength / BlockSize;

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

            var stream = new FileStream(
                fullPath,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.ReadWrite,
                bufferSize: 4096,
                options: FileOptions.Asynchronous | FileOptions.RandomAccess
            );
            _files.Add(new TorrentFileEntry(fullPath, offset, item.Length, stream));
            offset += item.Length;
        }
    }

    public List<RequestBlock> GetBlocksByPieceIndex(int pieceIndex)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(pieceIndex);

        // total number of pieces
        var pieceCount = (TotalSize + _pieceLength - 1) / _pieceLength;
        if (pieceIndex >= pieceCount)
            throw new ArgumentOutOfRangeException(nameof(pieceIndex));

        // compute actual piece length (last piece may be smaller)
        var pieceLength =
            (pieceIndex == pieceCount - 1)
                ? TotalSize - (long)pieceIndex * _pieceLength
                : _pieceLength;

        // number of blocks in this piece (ceiling division)
        int blockCount = (int)((pieceLength + BlockSize - 1) / BlockSize);
        var requests = new RequestBlock[blockCount];

        for (int i = 0; i < blockCount; i++)
        {
            var begin = (int)(i * BlockSize);
            var length = (int)Math.Min(BlockSize, pieceLength - begin);
            requests[i] = new RequestBlock(pieceIndex, begin, length);
        }

        return [.. requests];
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

    public async ValueTask<bool> VerifyPieceAsync(int pieceIndex, CancellationToken ct = default)
    {
        var expectedHash = _pieceHashes[pieceIndex];
        long offset = (long)pieceIndex * _pieceLength;
        int length = _pieceLength;

        // Read actual data back
        var actualData = await ReadAsync(offset, length, ct);
        var actualHash = SHA1.HashData(actualData);

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
                file.FileStream.SafeFileHandle,
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

            Directory.CreateDirectory(Path.GetDirectoryName(file.FullPath)!);

            await RandomAccess.WriteAsync(
                file.FileStream.SafeFileHandle,
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

    public async ValueTask<byte[]> ReadPieceAsync(
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

    private async ValueTask<byte[]> ReadAsync(long globalOffset, int length, CancellationToken ct)
    {
        var buffer = new byte[length];
        int totalRead = 0;

        foreach (var file in _files)
        {
            if (globalOffset >= file.EndOffset)
                continue;

            long fileOffset = Math.Max(0, globalOffset - file.StartOffset);
            long readable = Math.Min(length - totalRead, file.Length - fileOffset);

            // Perform thread-safe, offset-based async read
            int bytesRead = await RandomAccess.ReadAsync(
                file.FileStream.SafeFileHandle,
                buffer.AsMemory(totalRead, (int)readable),
                fileOffset,
                ct
            );

            totalRead += bytesRead;
            globalOffset += bytesRead;

            if (totalRead >= length || bytesRead == 0)
                break;
        }

        if (totalRead < length)
            Array.Resize(ref buffer, totalRead);

        return buffer;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var item in _files)
        {
            RandomAccess.FlushToDisk(item.FileStream.SafeFileHandle);
            await item.DisposeAsync();
        }
    }

    private sealed record TorrentFileEntry(
        string FullPath,
        long StartOffset,
        long Length,
        FileStream FileStream
    ) : IAsyncDisposable
    {
        public long EndOffset => StartOffset + Length;

        public async ValueTask DisposeAsync() => await FileStream.DisposeAsync();
    }
}
