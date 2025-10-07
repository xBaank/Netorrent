using System.Text;
using Netorrent.Bencoding;
using Netorrent.Bencoding.Structs;

namespace Netorrent.Tests;

public class BencodingTests
{
    [Theory]
    [InlineData("4:spam", "spam")]
    [InlineData("12:hello world!", "hello world!")]
    [InlineData("0:", "")]
    [InlineData("5:12345", "12345")]
    [InlineData("3:foo", "foo")]
    [InlineData("9:test data", "test data")]
    [InlineData("11:hello_world", "hello_world")]
    [InlineData("6:!@#$%^", "!@#$%^")]
    [InlineData("15:multi\nline\ndata", "multi\nline\ndata")]
    [InlineData("12:áéíóúñ", "áéíóúñ")]
    [InlineData("9:🙂emoji", "🙂emoji")]
    [InlineData("1:a", "a")]
    public void CanDecodeBString(string input, string actual)
    {
        var bytes = Encoding.UTF8.GetBytes(input);
        var decoder = new BDecoder(bytes.AsSpan());

        var decoded = decoder.Decode();

        Assert.True(decoded is BString);
        Assert.Equal(actual, decoded.ToString());
    }
}
