using System.Buffers;
using System.Security.Cryptography;
using Netorrent.Extensions;
using Netorrent.Other;
using Netorrent.TorrentFile.FileStructure;
using ZLinq;

namespace Netorrent.IO.Disk;

internal class DiskStorage : IPieceStorage
{
    private readonly string _outputDirectory;
    private readonly List<TorrentFileEntry> _files = [];
    private readonly int _pieceLength;
    private readonly IReadOnlyList<byte[]> _pieceHashes;

    public string OutputDirectory => _outputDirectory;

    public DiskStorage(
        string outputDirectory,
        IReadOnlyList<InfoFile> torrentFiles,
        int pieceLength,
        IReadOnlyList<byte[]> pieceHashes
    )
    {
        _outputDirectory = outputDirectory;
        _pieceLength = pieceLength;
        _pieceHashes = pieceHashes;

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
    }

    public async ValueTask WriteAsync(
        int pieceIndex,
        int begin,
        ReadOnlyMemory<byte> pieceData,
        CancellationToken ct = default
    )
    {
        long globalOffset = (long)pieceIndex * _pieceLength;
        globalOffset += begin;
        await WriteAsync(globalOffset, pieceData, ct).ConfigureAwait(false);
    }

    public bool VerifyPiece(int pieceIndex, ReadOnlyMemory<byte> pieceData)
    {
        var expectedHash = _pieceHashes[pieceIndex];
        Span<byte> actualHash = stackalloc byte[20];

        if (SHA1.TryHashData(pieceData.Span, actualHash, out _))
        {
            return expectedHash.SequenceEqual(actualHash);
        }

        return false;
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

            if (!file.IsDirectoryCreated)
            {
                Directory.CreateDirectory(Path.GetDirectoryName(file.FullPath)!);
                file.IsDirectoryCreated = true;
            }

            await RandomAccess
                .WriteAsync(file.SafeHandle, data.Slice(position, (int)writable), fileOffset, ct)
                .ConfigureAwait(false);

            globalOffset += writable;
            position += (int)writable;
            remaining -= writable;

            if (remaining <= 0)
                break;
        }
    }

    public async ValueTask<RentedArray<byte>> ReadAsync(
        int pieceIndex,
        int begin,
        int length,
        CancellationToken ct
    )
    {
        long offset = (long)pieceIndex * _pieceLength;
        offset += begin;
        return await ReadAsync(offset, length, ct).ConfigureAwait(false);
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

            int bytesRead = await RandomAccess
                .ReadAsync(file.SafeHandle, buffer.Slice(totalRead, (int)readable), fileOffset, ct)
                .ConfigureAwait(false);

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
}
