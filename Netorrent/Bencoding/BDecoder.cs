using System.Text;
using Netorrent.Bencoding.Structs;

namespace Netorrent.Bencoding;

public ref struct BDecoder
{
    private readonly ReadOnlySpan<byte> _data;
    private int _pos;

    public BDecoder(ReadOnlySpan<byte> data)
    {
        _data = data;
    }

    public IBencodingNode Decode()
    {
        var current = (char)_data[_pos];
        if (Char.IsAsciiDigit(current))
        {
            return DecodeString();
        }
        if (current == 'i')
        {
            return DecodeInt();
        }
        if (current == 'l')
        {
            return DecodeList();
        }
        if (current == 'd')
        {
            return DecodeDic();
        }

        throw new InvalidDataException();
    }

    public BString DecodeString()
    {
        var current = (char)_data[_pos];
        var startOffset = _pos;
        var length = 0;

        while (current != ':')
        {
            length++;
            current = (char)_data[++_pos];
        }

        if (int.TryParse(_data.Slice(startOffset, length), out var totalLength))
        {
            var result = Encoding.UTF8.GetString(_data.Slice(++_pos, totalLength));
            _pos += totalLength;
            return result;
        }
        else
            throw new InvalidDataException();
    }

    public BInt DecodeInt()
    {
        var current = (char)_data[++_pos];
        var startingOffset = _pos;
        var length = 0;

        while (current != 'e')
        {
            length++;
            current = (char)_data[++_pos];
        }

        var slice = _data.Slice(startingOffset, length);

        if (long.TryParse(slice, out var result))
        {
            _pos++;
            return result;
        }

        throw new InvalidDataException();
    }

    public BList DecodeList()
    {
        var list = new List<IBencodingNode>();
        _pos++;
        while ((char)_data[_pos] != 'e')
        {
            var item = Decode();
            list.Add(item);
        }
        _pos++;
        return list;
    }

    public BDictionary DecodeDic()
    {
        var dic = new Dictionary<BString, IBencodingNode>();
        _pos++;
        while ((char)_data[_pos] != 'e')
        {
            var key = DecodeString();
            var item = Decode();
            dic.Add(key, item);
        }
        _pos++;
        return dic;
    }
}
