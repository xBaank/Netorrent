using System.Buffers;
using System.Security.Cryptography;
using System.Threading.Channels;
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
            {
                Directory.CreateDirectory(folder);
            }

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

    public bool VerifyPieceHash(int pieceIndex, ref readonly ReadOnlySpan<byte> computedHash)
    {
        var expectedHash = _pieceHashes[pieceIndex];
        return expectedHash.SequenceEqual(computedHash);
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
            {
                continue;
            }

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
            {
                break;
            }
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
        var rentedArray = new RentedArray<byte>(length);
        try
        {
            int totalRead = 0;

            foreach (var file in _files)
            {
                if (globalOffset >= file.EndOffset)
                {
                    continue;
                }

                long fileOffset = Math.Max(0, globalOffset - file.StartOffset);
                long readable = Math.Min(length - totalRead, file.Length - fileOffset);

                int bytesRead = await RandomAccess
                    .ReadAsync(
                        file.SafeHandle,
                        rentedArray.Memory.Slice(totalRead, (int)readable),
                        fileOffset,
                        ct
                    )
                    .ConfigureAwait(false);

                totalRead += bytesRead;
                globalOffset += bytesRead;

                if (totalRead >= length || bytesRead == 0)
                {
                    break;
                }
            }

            if (totalRead < length)
            {
                length = totalRead;
            }

            return rentedArray;
        }
        catch
        {
            rentedArray.Dispose();
            throw;
        }
    }

    public IAsyncEnumerable<(int PieceIndex, bool IsValid)> CheckPiecesAsync(
        CancellationToken cancellationToken
    )
    {
        var resultsChannel = Channel.CreateBounded<(int PieceIndex, bool IsValid)>(
            new BoundedChannelOptions(64) { SingleReader = true, SingleWriter = false }
        );
        _ = RunPipelineAsync(resultsChannel.Writer, cancellationToken);
        return resultsChannel.Reader.ReadAllAsync(cancellationToken);
    }

    private async Task RunPipelineAsync(
        ChannelWriter<(int PieceIndex, bool IsValid)> resultsWriter,
        CancellationToken cancellationToken
    )
    {
        var piecesChannel = Channel.CreateBounded<(int PieceIndex, RentedArray<byte> Piece)>(
            new BoundedChannelOptions(64) { SingleReader = true, SingleWriter = true }
        );
        var readTask = ReadPiecesAsync(piecesChannel.Writer, cancellationToken);
        var verifyTask = VerifyPiecesAsync(piecesChannel.Reader, resultsWriter, cancellationToken);
        try
        {
            await Task.WhenAll(readTask, verifyTask).ConfigureAwait(false);
            resultsWriter.TryComplete();
        }
        catch (Exception ex)
        {
            resultsWriter.TryComplete(ex);
        }
    }

    private async Task ReadPiecesAsync(
        ChannelWriter<(int PieceIndex, RentedArray<byte> Piece)> writer,
        CancellationToken cancellationToken
    )
    {
        try
        {
            var pieceIndex = 0;
            var bufferPos = 0;
            using var pool = MemoryPool<byte>.Shared.Rent(_pieceLength);
            var pieceBuffer = pool.Memory[.._pieceLength];

            foreach (var file in _files)
            {
                var fileRemaining = file.Length;
                cancellationToken.ThrowIfCancellationRequested();

                await using var fs = new FileStream(
                    file.FullPath,
                    FileMode.OpenOrCreate,
                    FileAccess.Read,
                    FileShare.ReadWrite,
                    4096,
                    FileOptions.SequentialScan | FileOptions.Asynchronous
                );
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var toRead = (int)Math.Min(_pieceLength - bufferPos, fileRemaining);
                    if (toRead == 0)
                        break;
                    int bytesRead = await fs.ReadAsync(
                            pieceBuffer.Slice(bufferPos, toRead),
                            cancellationToken
                        )
                        .ConfigureAwait(false);
                    bufferPos += bytesRead;
                    fileRemaining -= bytesRead;
                    if (bytesRead == 0 || fileRemaining == 0)
                        break;

                    if (bufferPos == _pieceLength)
                    {
                        var rentedArray = new RentedArray<byte>(_pieceLength);
                        try
                        {
                            pieceBuffer.CopyTo(rentedArray.Memory);
                            await writer
                                .WriteAsync((pieceIndex, rentedArray), cancellationToken)
                                .ConfigureAwait(false);
                            pieceBuffer.Span.Clear();
                            pieceIndex++;
                            bufferPos = 0;
                        }
                        catch
                        {
                            rentedArray.Dispose();
                            throw;
                        }
                    }
                }
            }

            if (bufferPos > 0)
            {
                var rentedArray = new RentedArray<byte>(bufferPos);
                try
                {
                    pieceBuffer[..bufferPos].CopyTo(rentedArray.Memory);
                    await writer
                        .WriteAsync((pieceIndex, rentedArray), cancellationToken)
                        .ConfigureAwait(false);
                }
                catch
                {
                    rentedArray.Dispose();
                    throw;
                }
            }

            writer.TryComplete();
        }
        catch (Exception ex)
        {
            writer.TryComplete(ex);
            throw;
        }
    }

    private async Task VerifyPiecesAsync(
        ChannelReader<(int PieceIndex, RentedArray<byte> Piece)> reader,
        ChannelWriter<(int PieceIndex, bool IsValid)> writer,
        CancellationToken cancellationToken
    )
    {
        await Parallel
            .ForEachAsync(
                reader.ReadAllAsync(cancellationToken),
                new ParallelOptions
                {
                    CancellationToken = cancellationToken,
                    MaxDegreeOfParallelism = Environment.ProcessorCount,
                },
                async (item, ct) =>
                {
                    using (item.Piece)
                    {
                        ct.ThrowIfCancellationRequested();
                        var isValid = VerifyPiece(item.PieceIndex, item.Piece.Memory);
                        await writer
                            .WriteAsync((item.PieceIndex, isValid), ct)
                            .ConfigureAwait(false);
                    }
                }
            )
            .ConfigureAwait(false);
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
