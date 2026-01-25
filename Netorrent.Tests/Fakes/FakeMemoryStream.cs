using Netorrent.Extensions;
using R3;

namespace Netorrent.Tests.Fakes;

public sealed class FakeMemoryStream : Stream
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

    public override int Read(byte[] buffer, int offset, int count)
    {
        var read = _memoryStream.Read(buffer, offset, count);
        return read;
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
