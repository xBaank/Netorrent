using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using Netorrent.Bencoding.Structs;
using Netorrent.Exceptions;

namespace Netorrent.Bencoding;

internal sealed class BDecoder(Stream stream, int maxDepth = 64)
{
    private readonly PipeReader reader = PipeReader.Create(stream);

    public async ValueTask<IBencodingNode> DecodeAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            var result = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            var buffer = result.Buffer;

            if (buffer.Length == 0 && result.IsCompleted)
            {
                throw new EndOfStreamException();
            }

            var seqReader = new SequenceReader<byte>(buffer);

            if (TryDecode(ref seqReader, out var node, depth: 0))
            {
                reader.AdvanceTo(seqReader.Position);
                if (seqReader.Remaining > 0)
                {
                    throw new BencodingException("Extra data after root element");
                }
                return node;
            }

            if (result.IsCompleted)
            {
                throw new EndOfStreamException();
            }

            reader.AdvanceTo(buffer.Start, buffer.End);
        }
    }

    private bool TryDecode(
        ref SequenceReader<byte> reader,
        [NotNullWhen(true)] out IBencodingNode? node,
        int depth
    )
    {
        node = null;

        if (depth > maxDepth)
        {
            throw new BencodingException($"Maximum nesting depth of {maxDepth} exceeded");
        }

        if (!reader.TryPeek(out var b))
        {
            return false;
        }

        return b switch
        {
            >= (byte)'0' and <= (byte)'9' => TryDecodeString(ref reader, out node),
            (byte)'i' => TryDecodeInt(ref reader, out node),
            (byte)'l' => TryDecodeList(ref reader, out node, depth),
            (byte)'d' => TryDecodeDictionary(ref reader, out node, depth),
            _ => throw new BencodingException($"Invalid Token: {b}"),
        };
    }

    private static bool TryDecodeString(
        ref SequenceReader<byte> reader,
        [NotNullWhen(true)] out IBencodingNode? node
    )
    {
        node = null;

        if (!reader.TryReadTo(out ReadOnlySpan<byte> lenSpan, (byte)':'))
        {
            return false;
        }

        if (!int.TryParse(lenSpan, out var length))
        {
            throw new BencodingException("Can't decode BString");
        }

        if (!reader.TryReadExact(length, out var data))
        {
            return false;
        }

        node = new BString(data.ToArray());
        return true;
    }

    private static bool TryDecodeInt(
        ref SequenceReader<byte> reader,
        [NotNullWhen(true)] out IBencodingNode? node
    )
    {
        node = null;
        reader.Advance(1);

        if (!reader.TryReadTo(out ReadOnlySpan<byte> numSpan, (byte)'e'))
        {
            return false;
        }

        if (!IsValidBencodeInteger(numSpan) || !long.TryParse(numSpan, out var value))
        {
            throw new BencodingException("Can't decode BInt");
        }

        node = new BInt(value);
        return true;

        static bool IsValidBencodeInteger(ReadOnlySpan<byte> span)
        {
            if (span.Length == 0)
            {
                return false;
            }

            var first = (char)span[0];

            if (first == '+')
            {
                return false;
            }

            if (first == '-')
            {
                if (span.Length == 1)
                {
                    return false;
                }

                if (span[1] == '0')
                {
                    return false;
                }
            }
            else if (first == '0' && span.Length > 1)
            {
                return false;
            }

            for (int i = 0; i < span.Length; i++)
            {
                var c = (char)span[i];

                if (!char.IsAsciiDigit(c) && c != '-')
                {
                    return false;
                }
            }

            return true;
        }
    }

    private bool TryDecodeList(
        ref SequenceReader<byte> reader,
        [NotNullWhen(true)] out IBencodingNode? node,
        int depth
    )
    {
        node = null;
        reader.Advance(1);

        var list = new List<IBencodingNode>();

        while (true)
        {
            if (!reader.TryPeek(out var b))
            {
                return false;
            }

            if (b == (byte)'e')
            {
                reader.Advance(1);
                node = new BList(list);
                return true;
            }

            if (!TryDecode(ref reader, out var item, depth + 1))
            {
                return false;
            }

            list.Add(item);
        }
    }

    private bool TryDecodeDictionary(
        ref SequenceReader<byte> reader,
        [NotNullWhen(true)] out IBencodingNode? node,
        int depth
    )
    {
        node = null;
        reader.Advance(1);

        var dict = new Dictionary<BString, IBencodingNode>();

        while (true)
        {
            if (!reader.TryPeek(out var b))
            {
                return false;
            }

            if (b == (byte)'e')
            {
                reader.Advance(1);
                node = new BDictionary(dict);
                return true;
            }

            if (!TryDecodeString(ref reader, out var keyNode))
            {
                return false;
            }

            if (!TryDecode(ref reader, out var value, depth + 1))
            {
                return false;
            }

            dict.Add((BString)keyNode, value);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await stream.DisposeAsync().ConfigureAwait(false);
    }
}
