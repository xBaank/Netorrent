using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using Netorrent.Bencoding.Structs;

namespace Netorrent.Bencoding;

internal sealed class BEncoder(Stream stream) : IAsyncDisposable
{
    private readonly PipeWriter _writer = PipeWriter.Create(stream);
    private static readonly IComparer<byte[]> _bytewiseComparerInstance = Comparer<byte[]>.Create(
        BytewiseCompare
    );

    public async ValueTask EncodeAsync(IBencodingNode node, CancellationToken cancellationToken)
    {
        await EncodeNodeAsync(node, cancellationToken).ConfigureAwait(false);
        await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask EncodeNodeAsync(
        IBencodingNode node,
        CancellationToken cancellationToken
    )
    {
        switch (node)
        {
            case BString bs:
                await EncodeStringAsync(bs, cancellationToken).ConfigureAwait(false);
                break;
            case BInt bi:
                await EncodeIntAsync(bi, cancellationToken).ConfigureAwait(false);
                break;
            case BList bl:
                await EncodeListAsync(bl, cancellationToken).ConfigureAwait(false);
                break;
            case BDictionary bd:
                await EncodeDictionaryAsync(bd, cancellationToken).ConfigureAwait(false);
                break;
            default:
                throw new InvalidOperationException(
                    $"Unknown Bencoding type: {node.GetType().Name}"
                );
        }
    }

    private async ValueTask EncodeStringAsync(BString value, CancellationToken cancellationToken)
    {
        var bytes = value.RawData;
        var lengthBytes = Encoding.ASCII.GetBytes(bytes.Length.ToString());
        var span = _writer.GetSpan(lengthBytes.Length + 1); // +1 for colon
        lengthBytes.CopyTo(span);
        span[lengthBytes.Length] = (byte)':';
        _writer.Advance(lengthBytes.Length + 1);

        await _writer.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask EncodeIntAsync(BInt value, CancellationToken cancellationToken)
    {
        // Reserve max 21 bytes: 'i' + 20 digits + 'e' (long.MaxValue is 19 digits)
        var span = _writer.GetSpan(21);
        int len = Encoding.ASCII.GetBytes(value.Data.ToString(), span.Slice(1)); // skip first byte for 'i'
        span[0] = (byte)'i';
        span[len + 1] = (byte)'e';
        _writer.Advance(len + 2);

        await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask EncodeListAsync(BList list, CancellationToken cancellationToken)
    {
        var span = _writer.GetSpan(1);
        span[0] = (byte)'l';
        _writer.Advance(1);

        foreach (var item in list.Elements)
        {
            await EncodeNodeAsync(item, cancellationToken).ConfigureAwait(false);
        }

        span = _writer.GetSpan(1);
        span[0] = (byte)'e';
        _writer.Advance(1);
    }

    private async ValueTask EncodeDictionaryAsync(
        BDictionary dic,
        CancellationToken cancellationToken
    )
    {
        var span = _writer.GetSpan(1);
        span[0] = (byte)'d';
        _writer.Advance(1);

        foreach (var kvp in dic.Elements.OrderBy(k => k.Key.RawData, _bytewiseComparerInstance))
        {
            await EncodeStringAsync(kvp.Key, cancellationToken).ConfigureAwait(false);
            await EncodeNodeAsync(kvp.Value, cancellationToken).ConfigureAwait(false);
        }

        span = _writer.GetSpan(1);
        span[0] = (byte)'e';
        _writer.Advance(1);
    }

    private static int BytewiseCompare(byte[]? a, byte[]? b)
    {
        if (ReferenceEquals(a, b))
        {
            return 0;
        }

        if (a is null)
        {
            return -1;
        }

        if (b is null)
        {
            return 1;
        }

        int len = Math.Min(a.Length, b.Length);
        for (int i = 0; i < len; i++)
        {
            int diff = a[i].CompareTo(b[i]);
            if (diff != 0)
            {
                return diff;
            }
        }

        return a.Length.CompareTo(b.Length);
    }

    public async ValueTask DisposeAsync()
    {
        await _writer.CompleteAsync().ConfigureAwait(false);
    }
}
