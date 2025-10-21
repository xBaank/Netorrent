using System.Text;
using Netorrent.Bencoding;
using Netorrent.Bencoding.Structs;
using Netorrent.Tests.Data;
using Shouldly;

namespace Netorrent.Tests.Bencoding;

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

        var decoded = (BString)decoder.Decode();

        decoded.Data.ShouldBeEquivalentTo(actual);
    }

    [Theory]
    [InlineData("i0e", 0)]
    [InlineData("i1e", 1)]
    [InlineData("i42e", 42)]
    [InlineData("i-1e", -1)]
    [InlineData("i999e", 999)]
    [InlineData("i123456789e", 123456789)]
    [InlineData("i-99999e", -99999)]
    [InlineData("i2147483647e", 2147483647)] // max 32-bit int
    [InlineData("i-2147483648e", -2147483648)] // min 32-bit int
    [InlineData("i9223372036854775807e", 9223372036854775807L)] // max 64-bit
    [InlineData("i-9223372036854775808e", -9223372036854775808L)] // min 64-bit
    [InlineData("i007e", 7)] // technically invalid in strict bencoding (leading zeros), but useful for tests
    [InlineData("i-0e", 0)] // another edge case: negative zero normalization
    [InlineData("i000000e", 0)] // leading zeros case
    public void CanDecodeBInt(string input, long actual)
    {
        var bytes = Encoding.UTF8.GetBytes(input);
        var decoder = new BDecoder(bytes.AsSpan());

        var decoded = (BInt)decoder.Decode();

        decoded.Data.ShouldBeEquivalentTo(actual);
    }

    [Theory]
    [ClassData(typeof(BDictionaryData))]
    [ClassData(typeof(BlistData))]
    public void CanDecodeNonPrimitives(string input, IBencodingType actual)
    {
        var bytes = Encoding.UTF8.GetBytes(input);
        var decoder = new BDecoder(bytes.AsSpan());

        var decoded = decoder.Decode();

        decoded.ShouldBeEquivalentTo(actual);
    }
}
