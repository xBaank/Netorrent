using Netorrent.Extensions;
using R3;

namespace Netorrent.Tests.Fakes;

public sealed class FakeMemoryStream(bool infiniteHold = false) : Stream
{
    private readonly MemoryStream _memoryStream = new();
    public Subject<ReadOnlyMemory<byte>> WrittenData { get; } = new();

    public override bool CanRead => true;

    public override bool CanSeek => false;

    public override bool CanWrite => true;

    public override long Length => throw new NotImplementedException();

    public override long Position
    {
        get => throw new NotImplementedException();
        set => throw new NotImplementedException();
    }

    public override void Flush()
    {
        _memoryStream.Flush();
        _memoryStream.Position = 0;
    }

    public override async ValueTask<int> ReadAsync(
        Memory<byte> buffer,
        CancellationToken cancellationToken = default
    )
    {
        if (infiniteHold)
        {
            await Task.Delay(-1, cancellationToken);
        }
        return await _memoryStream.ReadAsync(buffer, cancellationToken);
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        return _memoryStream.Read(buffer, offset, count);
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotImplementedException();
    }

    public override void SetLength(long value)
    {
        throw new NotImplementedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        WrittenData.OnNext(buffer.AsMemory(offset, count));
        _memoryStream.Write(buffer, offset, count);
    }

    public override ValueTask DisposeAsync()
    {
        WrittenData.Dispose();
        return base.DisposeAsync();
    }
}
