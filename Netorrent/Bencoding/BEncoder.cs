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
        var bytes = Encoding.UTF8.GetBytes(value);
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

        foreach (var kvp in dic.Elements.OrderBy(k => k.Key.Data, StringComparer.Ordinal))
        {
            EncodeString(kvp.Key);
            EncodeToStream(kvp.Value);
        }

        stream.WriteByte((byte)'e');
    }

    public async ValueTask DisposeAsync() => await stream.DisposeAsync();
}
