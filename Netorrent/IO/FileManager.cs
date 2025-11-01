namespace Netorrent.IO;

using System.Collections;
using System.Security.Cryptography;
using Netorrent.P2P.Structs;
using Netorrent.TorrentFile.FileStructure;

internal class FileManager : IAsyncDisposable
{
    private readonly string _outputDirectory;
    private readonly List<TorrentFileEntry> _files = [];
    private readonly int _pieceLength;
    private readonly List<byte[]> _pieceHashes;
    public Bitfield BitField { get; private set; }

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
            var stream = new FileStream(
                fullPath,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.Read,
                bufferSize: 4096,
                options: FileOptions.Asynchronous | FileOptions.RandomAccess
            );
            _files.Add(new TorrentFileEntry(fullPath, offset, item.Length, stream));
            offset += item.Length;
        }
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
        byte[] pieceData,
        CancellationToken ct = default
    )
    {
        long globalOffset = (long)pieceIndex * _pieceLength;

        await WriteAsync(globalOffset, pieceData, ct);

        bool ok = await VerifyPieceAsync(pieceIndex, ct);
        if (!ok)
        {
            // Optionally delete or mark as bad
            throw new InvalidDataException($"Piece {pieceIndex} failed hash verification.");
        }
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

    private async ValueTask WriteAsync(long globalOffset, byte[] data, CancellationToken ct)
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

            file.FileStream.Seek(fileOffset, SeekOrigin.Begin);
            await file.FileStream.WriteAsync(data.AsMemory(position, (int)writable), ct);

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

            file.FileStream.Seek(fileOffset, SeekOrigin.Begin);
            int bytesRead = await file.FileStream.ReadAsync(
                buffer.AsMemory(totalRead, (int)readable),
                ct
            );

            totalRead += bytesRead;
            globalOffset += bytesRead;

            if (totalRead >= length)
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
            await item.DisposeAsync();
        }
    }

    private sealed record TorrentFileEntry(
        string FullPath,
        long StartOffset,
        long Length,
        Stream FileStream
    ) : IAsyncDisposable
    {
        public long EndOffset => StartOffset + Length;

        public async ValueTask DisposeAsync() => await FileStream.DisposeAsync();
    }
}
