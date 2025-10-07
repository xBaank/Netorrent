using System.Text;
using Netorrent.Bencoding;
using Netorrent.Bencoding.Structs;

namespace Netorrent.Tests;

public class BencodingTests
{
    [Fact]
    public void CanDecodeBString()
    {
        var data = "12:hello world!";
        var bytes = Encoding.ASCII.GetBytes(data);
        var decoder = new BDecoder(bytes.AsSpan());

        var decoded = decoder.Decode();

        Assert.True(decoded is BString);
        Assert.Equal("hello world!", decoded.ToString());
    }
}
