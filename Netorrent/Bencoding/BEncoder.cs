using System.Text;
using Netorrent.Bencoding.Structs;

namespace Netorrent.Bencoding;

//TODO change impl to prealocated span instead of using memory stream
public sealed class BEncoder : IAsyncDisposable
{
    private readonly MemoryStream stream;

    public BEncoder()
    {
        stream = new MemoryStream(4096);
    }

    public byte[] Encode(IBencodingNode value)
    {
        EncodeToStream(value);
        return stream.ToArray();
    }

    private void EncodeToStream(IBencodingNode value)
    {
        switch (value)
        {
            case BString bs:
                EncodeString(bs);
                break;
            case BInt bi:
                EncodeInt(bi);
                break;
            case BList bl:
                EncodeList(bl);
                break;
            case BDictionary bd:
                EncodeDictionary(bd);
                break;
            default:
                throw new InvalidOperationException(
                    $"Unknown Bencoding type: {value.GetType().Name}"
                );
        }
    }

    private void EncodeString(BString value)
    {
        var bytes = value.RawData;
        var lengthBytes = Encoding.ASCII.GetBytes(bytes.Length.ToString() + ":");
        stream.Write(lengthBytes, 0, lengthBytes.Length);
        stream.Write(bytes, 0, bytes.Length);
    }

    private void EncodeInt(BInt value)
    {
        var bytes = Encoding.ASCII.GetBytes($"i{value.Data}e");
        stream.Write(bytes, 0, bytes.Length);
    }

    private void EncodeList(BList list)
    {
        stream.WriteByte((byte)'l');
        foreach (var item in list.Elements)
        {
            EncodeToStream(item);
        }
        stream.WriteByte((byte)'e');
    }

    private void EncodeDictionary(BDictionary dic)
    {
        stream.WriteByte((byte)'d');

        foreach (
            var kvp in dic.Elements.OrderBy(
                k => k.Key.RawData,
                Comparer<byte[]>.Create(BytewiseCompare)
            )
        )
        {
            EncodeString(kvp.Key);
            EncodeToStream(kvp.Value);
        }

        stream.WriteByte((byte)'e');
    }

    private static int BytewiseCompare(byte[]? a, byte[]? b)
    {
        if (ReferenceEquals(a, b))
            return 0;
        if (a is null)
            return -1;
        if (b is null)
            return 1;

        int len = Math.Min(a.Length, b.Length);
        for (int i = 0; i < len; i++)
        {
            int diff = a[i].CompareTo(b[i]);
            if (diff != 0)
                return diff;
        }
        return a.Length.CompareTo(b.Length);
    }

    public async ValueTask DisposeAsync() => await stream.DisposeAsync();
}
