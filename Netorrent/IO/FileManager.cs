namespace Netorrent.IO;

using System.Collections;
using System.Security.Cryptography;
using Netorrent.TorrentFile.FileStructure;

internal class FileManager
{
    private readonly string _outputDirectory;
    private readonly List<TorrentFileEntry> _files = [];
    private readonly int _pieceLength;
    private readonly List<byte[]> _pieceHashes;
    public BitArray BitField { get; private set; }

    public FileManager(
        string outputDirectory,
        List<InfoFile> torrentFiles,
        int pieceLength,
        List<byte[]> pieceHashes,
        BitArray bitField
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
            _files.Add(new TorrentFileEntry(fullPath, offset, item.Length));
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

    public async Task WritePieceAsync(
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

    public async Task<bool> VerifyPieceAsync(int pieceIndex, CancellationToken ct = default)
    {
        var expectedHash = _pieceHashes[pieceIndex];
        long offset = (long)pieceIndex * _pieceLength;
        int length = _pieceLength;

        // Read actual data back
        var actualData = await ReadAsync(offset, length, ct);
        var actualHash = SHA1.HashData(actualData);

        return expectedHash.SequenceEqual(actualHash);
    }

    private async Task WriteAsync(long globalOffset, byte[] data, CancellationToken ct)
    {
        long remaining = data.Length;
        int position = 0;

        foreach (var entry in _files)
        {
            if (globalOffset >= entry.EndOffset)
                continue;

            long fileOffset = Math.Max(0, globalOffset - entry.StartOffset);
            long writable = Math.Min(remaining, entry.Length - fileOffset);

            Directory.CreateDirectory(Path.GetDirectoryName(entry.FullPath)!);

            using var stream = new FileStream(
                entry.FullPath,
                FileMode.OpenOrCreate,
                FileAccess.Write,
                FileShare.Read,
                bufferSize: 4096,
                useAsync: true
            );

            stream.Seek(fileOffset, SeekOrigin.Begin);
            await stream.WriteAsync(data.AsMemory(position, (int)writable), ct);

            globalOffset += writable;
            position += (int)writable;
            remaining -= writable;

            if (remaining <= 0)
                break;
        }
    }

    private async Task<byte[]> ReadAsync(long globalOffset, int length, CancellationToken ct)
    {
        var buffer = new byte[length];
        int totalRead = 0;

        foreach (var entry in _files)
        {
            if (globalOffset >= entry.EndOffset)
                continue;

            long fileOffset = Math.Max(0, globalOffset - entry.StartOffset);
            long readable = Math.Min(length - totalRead, entry.Length - fileOffset);

            using var stream = new FileStream(
                entry.FullPath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite,
                bufferSize: 4096,
                useAsync: true
            );

            stream.Seek(fileOffset, SeekOrigin.Begin);
            int bytesRead = await stream.ReadAsync(buffer.AsMemory(totalRead, (int)readable), ct);

            totalRead += bytesRead;
            globalOffset += bytesRead;

            if (totalRead >= length)
                break;
        }

        if (totalRead < length)
            Array.Resize(ref buffer, totalRead);

        return buffer;
    }

    private sealed record TorrentFileEntry(string FullPath, long StartOffset, long Length)
    {
        public long EndOffset => StartOffset + Length;
    }
}
